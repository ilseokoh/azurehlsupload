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
                if (!reqItem.primaryUrl.ToLower().EndsWith(".m3u8")) throw new ArgumentException("url must be m3u8.");

                parser = new M3u8Parser(reqItem.primaryUrl);
                var entry = await parser.ParseEntry();

                // log first 
                logtable.CreateIfNotExists();

                M3u8PaserLogEntry entrylog = new M3u8PaserLogEntry(entry.Url);
                // root m3u8(1) + secondary m3u8 count + playlist count
                entrylog.TsCount = entry.Playlists.Sum(x => x.TsFiles.Count) + reqItem.secondaryUrls.Count + 1;
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

                if (uploadrequestcount != entrylog.TsCount) throw new InvalidOperationException("Difference between requested count and ts files for upload.");
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

                    // First page needs preload
                    if (i == 0)
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
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("AzureWebJobsStorage"));

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("webroot");
            await container.CreateIfNotExistsAsync();

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
                    
                    var stream = await client.GetStreamAsync(uri);
                    // Retrieve reference to a blob named "myblob".
                    CloudBlockBlob blockBlob = container.GetBlockBlobReference(path + "/" + filename);
                    await blockBlob.UploadFromStreamAsync(stream);
                }

                entrylog.IsSuccess = true;
                entrylog.Duration = DateTime.UtcNow.Subtract(startTime).TotalSeconds;

                TableOperation insertOperation = TableOperation.InsertOrMerge(entrylog);
                uploadlog.Execute(insertOperation);
            }
        }

        private async static Task UploadBlob(M3u8Entry entry)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("AzureWebJobsStorage"));

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("webroot");
            await container.CreateIfNotExistsAsync();

            string path = entry.Path;
            if (path.StartsWith("/")) path = path.Substring(1);

            foreach (var playlist in entry.Playlists)
            {
                using (HttpClient client = new HttpClient())
                {
                    var stream = await client.GetStreamAsync(playlist.Url);
                    // Retrieve reference to a blob named "myblob".
                    CloudBlockBlob blockBlob = container.GetBlockBlobReference(path + "/" + playlist.Filename);
                    await blockBlob.UploadFromStreamAsync(stream);
                }
            }

            // upload itself. 
            using (HttpClient client = new HttpClient())
            {
                var stream = await client.GetStreamAsync(entry.Url);
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(path + "/" + entry.Filename);
                await blockBlob.UploadFromStreamAsync(stream);
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

        public int TsCount { get; set; }

        public int UploadedTsCount { get; set; }

        public int PreloadRequestCount { get; set; }

        public int PreloadedTsCount { get; set; }

        public bool IsUploadComplete { get; set; }

        public bool HasError { get; set; }
    }

}
