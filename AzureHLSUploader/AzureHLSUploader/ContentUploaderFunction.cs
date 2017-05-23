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

namespace AzureHLSUploader
{
    public static class ContentUploaderFunction
    {
        [FunctionName("ContentUploader")]        
        public async static Task Run([QueueTrigger("uploadqueue", Connection = "AzureWebJobsStorage")]string queueitem,
                                [Table(tableName: "uploadlog", Connection = "AzureWebJobsStorage")]CloudTable logtable,
                                TraceWriter log)
        {
            List<string> items;
            try
            {
                items = JsonConvert.DeserializeObject<List<string>>(queueitem);
                log.Info($"----- Upload Contnent: {items.Count}");
            }
            catch(Exception ex)
            {
                throw new ArgumentException("Message from queue has error", ex);
            }

            // create log table
            logtable.CreateIfNotExists();

            foreach (var item in items)
            {
                UploadLogEntry entrylog = new UploadLogEntry(item);
                TableOperation insertOperation = TableOperation.InsertOrMerge(entrylog);
                logtable.Execute(insertOperation);

                try
                {
                    // retry 3 times with 1 sec delay
                    await RetryHelper.RetryOnExceptionAsync(3, TimeSpan.FromSeconds(1), async () =>
                    {
                        await UploadBlob(item);
                    });
                    entrylog.IsUploadComplete = true;
                    logtable.Execute(insertOperation);

                    log.Info($"------ Upload successfully: {item}");
                }
                catch(Exception ex)
                {
                    log.Error($"****** Upload failed: {item} : {ex.Message}");
                }
            }
        }

        private async static Task UploadBlob(string url)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                                CloudConfigurationManager.GetSetting("AzureWebJobsStorage"));

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
        public UploadLogEntry(string url)
        {
            this.PartitionKey = "upload";
            this.RowKey = url.Replace('/','_');
            IsUploadComplete = false;
        }

        public bool IsUploadComplete { get; set; }

    }

    
}
