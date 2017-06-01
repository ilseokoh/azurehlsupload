using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Blob;
using AzureHLSUploader.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.Azure;
using System.Net.Http;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using M3u8Parser.Utils;

namespace AzureHLSUploader
{
    public static class M3u8UrlParseFunction
    {
        [FunctionName("M3u8UrlParser")]
        public async static Task Run([QueueTrigger("m3u8queue", Connection = "AzureWebJobsStorage")]string req,
                                [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable logtable,
                                [Queue(queueName: "uploadqueue", Connection = "AzureWebJobsStorage")]CloudQueue uploadqueue,
                                [Table(tableName: "uploadlog", Connection = "AzureWebJobsStorage")]CloudTable uploadlog,
                                TraceWriter log)
        {
            RequestItem reqItem = null;
            try
            {
                reqItem = JsonConvert.DeserializeObject<RequestItem>(req);
                log.Info($"----- M3U8 Request: {reqItem.primaryUrl} (Secondary Url: {reqItem.secondaryUrls.Count})");
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Message from queue has error", ex);
            }

            M3u8Parser parser;
            try
            {
                if (string.IsNullOrEmpty(reqItem.primaryUrl ) || !reqItem.primaryUrl.ToLower().EndsWith(".m3u8") || !reqItem.primaryUrl.ToLower().StartsWith("http")) throw new ArgumentException("url must be m3u8.");

                parser = new M3u8Parser(reqItem.primaryUrl);
                var entry = await parser.ParseEntry();

                // log first 
                logtable.CreateIfNotExists();

                M3u8PaserLogEntry entrylog = new M3u8PaserLogEntry(entry.Url);
                // root m3u8(1) + secondary m3u8 count + playlist count
                entrylog.TotlaFileCount = entry.Playlists.Sum(x => x.TsFiles.Count) + entry.Playlists.Count + reqItem.secondaryUrls.Count + 1;
                entrylog.BitrateCount = entry.Playlists.Count;
                TableOperation insertOperation = TableOperation.InsertOrMerge(entrylog);
                logtable.Execute(insertOperation);

                // upload m3u8 files 
                List<string> uploadItems = new List<string>(entry.Playlists.Select(x => x.Url)); 
                foreach(var url in reqItem.secondaryUrls)
                {
                    uploadItems.Add(url);
                }
                // itself 
                uploadItems.Add(entry.Url);

                // retry 3 times with 1 sec delay
                await RetryHelper.RetryOnExceptionAsync(3, TimeSpan.FromSeconds(1), async () =>
                {
                    await UploadBlob(uploadItems, uploadlog, entry.Url);
                });

                entrylog.IsPlaylistUploadComplete = true;
                logtable.Execute(insertOperation);

                log.Info($"Upload all playlist(m3u8) files complete.");

                // queue upload request 
                int uploadrequestcount = await QueueUploadItems(entry, uploadqueue, entrylog);

                entrylog.IsUploadQueueComplete = true;
                logtable.Execute(insertOperation);

                log.Info($"Upload Request complete.");
            }
            catch(Exception ex)
            {
                log.Info("***** " + ex.Message);
            }
        }

        private async static Task<int> QueueUploadItems(M3u8Entry entry, CloudQueue uploadqueue, M3u8PaserLogEntry entrylog)
        {
            int pagesize = 10;
            int reqcount = 0;

            await uploadqueue.CreateIfNotExistsAsync();

            var baseurl = entry.BaseUrl + entry.Path;
            foreach(var playlist in entry.Playlists.OrderBy(x => x.Bandwidth))
            {
                double count = (double)(playlist.TsFiles.Count) / (double)pagesize;
                int totalpages = (int)Math.Ceiling(count);
                for(var i = 0; i < totalpages ; i++)
                {
                    UploadItem uploaditems = new UploadItem
                    {
                        Items = playlist.TsFiles.OrderBy(x => x).Skip(i * pagesize).Take(pagesize).Select(x => baseurl + "/" + x).ToList(),
                        Url = entry.Url
                    };

                    // preload for 10 pages
                    //if (i < 10)
                    if (i < 2)
                    {
                        uploaditems.NeedPreload = true;
                        entrylog.PreloadRequestCount += uploaditems.Items.Count;
                    }
                    else uploaditems.NeedPreload = false; 

                    CloudQueueMessage message = new CloudQueueMessage(JsonConvert.SerializeObject(uploaditems));

                    // retry 3 times with 1 sec delay
                    await RetryHelper.RetryOnExceptionAsync(3, TimeSpan.FromSeconds(1), async () =>
                    {
                        await uploadqueue.AddMessageAsync(message);
                    });

                    reqcount += uploaditems.Items.Count();
                }
            }
            return reqcount;
        }

        private async static Task UploadBlob(List<string> items, CloudTable uploadlog, string rootlogkey)
        {
            // Primary Storage
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("AzureWebJobsStorage"));
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("webroot");
            await container.CreateIfNotExistsAsync();

            // Secondary Storage
            CloudStorageAccount secondaryStorageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("SecondStorage"));
            CloudBlobClient secondaryBlobClient = secondaryStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer secondaryContainer = secondaryBlobClient.GetContainerReference("webroot");
            await secondaryContainer.CreateIfNotExistsAsync();

            foreach (var item in items)
            {
                // log
                UploadLogEntry entrylog = new UploadLogEntry(rootlogkey, item);

                Uri uri = new Uri(item);
                var idx = uri.AbsolutePath.LastIndexOf('/');
                var path = uri.AbsolutePath.Substring(1, idx);
                var filename = uri.AbsolutePath.Substring(idx + 1);

                DateTime startTime = DateTime.UtcNow;
                
                using (HttpClient client = new HttpClient())
                {
                    // download
                    byte[] bytes = await client.GetByteArrayAsync(uri);

                    // Copy to primary storage
                    CloudBlockBlob blockBlob = container.GetBlockBlobReference(path + filename);
                    await blockBlob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);

                    // Copy to secondary storage
                    CloudBlockBlob secondaryBlockBlob = secondaryContainer.GetBlockBlobReference(path + filename);
                    await secondaryBlockBlob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);
                }

                entrylog.IsSuccess = true;
                entrylog.Duration = DateTime.UtcNow.Subtract(startTime).TotalSeconds;

                TableOperation insertOperation = TableOperation.InsertOrMerge(entrylog);
                uploadlog.Execute(insertOperation);
            }
        }
    }

    public class M3u8PaserLogEntry: TableEntity
    {
        public M3u8PaserLogEntry(string url)
        {
            this.PartitionKey = "m3u8";
            this.RowKey = EscapeTablekey.Replace(url);

            Url = url;

            IsPlaylistUploadComplete = false;
            IsUploadQueueComplete = false;
            IsUploadComplete = false;
            HasError = false;
        }

        public M3u8PaserLogEntry() { }

        public string Url { get; set; }

        public bool IsPlaylistUploadComplete { get; set; }

        public bool IsUploadQueueComplete { get; set; }

        public int BitrateCount { get; set; }

        public int TotlaFileCount { get; set; }

        public int UploadedCount { get; set; }

        public int PreloadRequestCount { get; set; }

        public int PreloadedTsCount { get; set; }

        public bool IsUploadComplete { get; set; }

        public bool HasError { get; set; }

        public string OriginRequest { get; set; }
    }

}
