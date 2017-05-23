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
        public async static Task Run([QueueTrigger("m3u8queue", Connection = "AzureWebJobsStorage")]string url,
                                [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable logtable,
                                [Queue(queueName: "uploadqueue", Connection = "AzureWebJobsStorage")]CloudQueue uploadqueue,
                                [Queue(queueName: "preloadqueue", Connection = "AzureWebJobsStorage")]CloudQueue preloadqueue,
                                TraceWriter log)
        {
            log.Info($"------- Queue trigger function processed: {url}");

            M3u8Parser parser;
            try
            {
                parser = new M3u8Parser(url);
                var entry = await parser.ParseEntry();
                // log first 
                logtable.CreateIfNotExists();
                M3u8PaserLogEntry entrylog = new M3u8PaserLogEntry(entry.Path, entry.Filename);
                entrylog.Url = entry.Url;
                entrylog.TsCount = entry.Playlists.Sum(x => x.TsFiles.Count);
                entrylog.BitrateCount = entry.Playlists.Count;
                TableOperation insertOperation = TableOperation.InsertOrMerge(entrylog);
                logtable.Execute(insertOperation);

                // upload m3u8 files 
                // retry 3 times with 1 sec delay
                await RetryHelper.RetryOnExceptionAsync(3, TimeSpan.FromSeconds(1), async () =>
                {
                    await UploadBlob(entry);
                });
                entrylog.IsPlaylistUploadComplete = true;
                logtable.Execute(insertOperation);

                // Enqueue ts files (30)
                // retry 3 times with 1 sec delay
                await RetryHelper.RetryOnExceptionAsync(3, TimeSpan.FromSeconds(1), async () =>
                {
                    await QueueUploadItems(entry, uploadqueue);
                });
                entrylog.IsUploadQueueComplete = true;
                logtable.Execute(insertOperation);

                // Enueue pre-load files (10)
                // retry 3 times with 1 sec delay
                await RetryHelper.RetryOnExceptionAsync(3, TimeSpan.FromSeconds(1), async () =>
                {
                    await QueuePreloadItems(entry, preloadqueue);
                });
                entrylog.IsPreloadQueueComplete = true;
                logtable.Execute(insertOperation);
            }
            catch(Exception ex)
            {
                log.Info("--------- " + ex.Message);
            }
        }

        private async static Task QueueUploadItems(M3u8Entry entry, CloudQueue uploadqueue)
        {
            int pagesize = 30;
            await uploadqueue.CreateIfNotExistsAsync();

            var baseurl = entry.BaseUrl + entry.Path;
            foreach(var playlist in entry.Playlists.OrderBy(x => x.Bandwidth))
            {
                double count = (double)(playlist.TsFiles.Count) / (double)pagesize;
                int totalpages = (int)Math.Ceiling(count);
                for(var i = 0; i < totalpages ; i++)
                {
                    List<string> items = new List<string>();
                    CloudQueueMessage message = new CloudQueueMessage(JsonConvert.SerializeObject(playlist.TsFiles.Skip(i * pagesize).Take(pagesize).Select(x => baseurl + "/" + x).ToArray()));

                    // retry 3 times with 1 sec delay
                    await RetryHelper.RetryOnExceptionAsync(3, TimeSpan.FromSeconds(1), async () =>
                    {
                        await uploadqueue.AddMessageAsync(message);
                    });
                }
            }
        }

        private async static Task QueuePreloadItems(M3u8Entry entry, CloudQueue preloadqueue)
        {
            int pagesize = 10;
            await preloadqueue.CreateIfNotExistsAsync();

            var baseurl = entry.BaseUrl + entry.Path;
            // Highest bitrate
            var selectedList = entry.Playlists.OrderByDescending(x => x.Bandwidth).Take(2);
            foreach (var playlist in selectedList)
            {
                CloudQueueMessage message = new CloudQueueMessage(JsonConvert.SerializeObject(playlist.TsFiles.OrderBy(x => x).Take(pagesize).Select(x => baseurl + "/" + x).ToArray()));
                await preloadqueue.AddMessageAsync(message);
            }
        }

        private async static Task UploadBlob(M3u8Entry entry)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                                CloudConfigurationManager.GetSetting("TargetStorageAccount"));

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("webroot");
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

        }
    }

    public class M3u8PaserLogEntry: TableEntity
    {

        public M3u8PaserLogEntry(string path, string filename)
        {
            this.PartitionKey = path.Replace('/','_');
            this.RowKey = filename;
            IsPlaylistUploadComplete = false;
            IsUploadQueueComplete = false;
            IsPreloadQueueComplete = false;
        }

        public M3u8PaserLogEntry() { }

        public string Url { get; set; }

        public bool IsPlaylistUploadComplete { get; set; }

        public bool IsUploadQueueComplete { get; set; }

        public bool IsPreloadQueueComplete { get; set; }

        public int TsCount { get; set; }

        public int BitrateCount { get; set; }

        public int InsertedCount { get; set; }
    }
}
