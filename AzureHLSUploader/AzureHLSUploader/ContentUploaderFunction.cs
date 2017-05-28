using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Net.Http;
using M3u8Parser.Utils;
using AzureHLSUploader.Models;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Queue;

namespace AzureHLSUploader
{
    public static class ContentUploaderFunction
    {
        [FunctionName("ContentUploader")]        
        public async static Task Run([QueueTrigger("uploadqueue", Connection = "AzureWebJobsStorage")]string queueitem,
                                [Table(tableName: "uploadlog", Connection = "AzureWebJobsStorage")]CloudTable logtable,
                                [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable rootlogtable,
                                [Queue(queueName: "preloadqueue", Connection = "AzureWebJobsStorage")]CloudQueue preloadqueue,
                                TraceWriter log)
        {
            UploadItem uploaditem = null;
            try
            {
                uploaditem = JsonConvert.DeserializeObject<UploadItem>(queueitem);
                log.Info($"----- Start to upload Contnent: {uploaditem.Items.Count}");
            }
            catch(Exception ex)
            {
                throw new ArgumentException("Message from queue has error", ex);
            }

            // create log table
            await logtable.CreateIfNotExistsAsync();

            foreach (var item in uploaditem.Items)
            {
                // log
                UploadLogEntry entrylog = new UploadLogEntry(uploaditem.Url, item);

                try
                {
                    DateTime startTime = DateTime.UtcNow;
                    // retry 3 times with 1 sec delay
                    await RetryHelper.RetryOnExceptionAsync(3, TimeSpan.FromSeconds(1), async () =>
                    {
                        await UploadBlob(item);
                    });

                    entrylog.IsSuccess = true;
                    entrylog.Duration = DateTime.UtcNow.Subtract(startTime).TotalSeconds;

                    log.Info($"upload complete: {item}");
                    
                }
                catch(Exception ex)
                {
                    // error log
                    entrylog.IsSuccess = false;
                    entrylog.ErrorMessage = ex.Message;

                    log.Error($"****** Upload failed: {item} : {ex.Message}");
                }

                TableOperation insertOperation = TableOperation.InsertOrMerge(entrylog);
                logtable.Execute(insertOperation);
            }

            // Reqeust preload here! 
            if (uploaditem.NeedPreload == true)
            {
                await QueuePreloadItems(uploaditem, preloadqueue);
                log.Info($"Reqeust preload: {uploaditem.Items.Count}");
            }
            log.Info($"----- All complete: {uploaditem.Items.Count}");

            // Check result and log to root table log 
            TableQuery<M3u8PaserLogEntry> entryquery = new TableQuery<M3u8PaserLogEntry>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, EscapeTablekey.Replace(uploaditem.Url)));
            var m3u8entrylog = rootlogtable.ExecuteQuery(entryquery).FirstOrDefault();

            // Count completed items. 
            TableQuery<UploadLogEntry> uploaditemquery = new TableQuery<UploadLogEntry>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, EscapeTablekey.Replace(uploaditem.Url)));

            var uploadcount = logtable.ExecuteQuery(uploaditemquery).Where(x => x.IsSuccess == true).Count();
            var errorcount = logtable.ExecuteQuery(uploaditemquery).Where(x => x.IsSuccess == false).Count();

            if (m3u8entrylog == null) throw new InvalidOperationException("there is no m3u8 entry log on the table.");
            m3u8entrylog.UploadedTsCount = uploadcount;
            // check upload complete
            if (m3u8entrylog.TotlaFileCount == uploadcount) m3u8entrylog.IsUploadComplete = true;
            // check error count
            if (errorcount > 0) m3u8entrylog.HasError = true;

            TableOperation updateOperation = TableOperation.InsertOrMerge(m3u8entrylog);
            rootlogtable.Execute(updateOperation);
        }

        private async static Task QueuePreloadItems(UploadItem item, CloudQueue preloadqueue)
        {
            await preloadqueue.CreateIfNotExistsAsync();

            CloudQueueMessage message = new CloudQueueMessage(JsonConvert.SerializeObject(item));
            // retry 3 times with 1 sec delay
            await RetryHelper.RetryOnExceptionAsync(3, TimeSpan.FromSeconds(1), async () =>
            {
                await preloadqueue.AddMessageAsync(message);
            });
        }

        private async static Task UploadBlob(string url)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("AzureWebJobsStorage"));

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("webroot");

            Uri uri = new Uri(url);
            string path = uri.AbsolutePath.Substring(1);

            using (HttpClient client = new HttpClient())
            {
                // download
                var stream = await client.GetStreamAsync(url);
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(path);
                // upload
                await blockBlob.UploadFromStreamAsync(stream);
            }
        }
    }

    public class UploadLogEntry : TableEntity
    {
        public UploadLogEntry(string rooturl, string url)
        {
            this.PartitionKey = EscapeTablekey.Replace(rooturl);
            this.RowKey = EscapeTablekey.Replace(url);

            IsSuccess = false;
            ErrorMessage = "";
            Duration = 0;
        }

        public UploadLogEntry() { }

        public bool IsSuccess { get; set; }

        public double Duration { get; set; }

        public string ErrorMessage { get; set; }
    }

    
}
