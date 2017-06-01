using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq;

namespace AzureHLSErrorCollector
{
    public static class RetryCopyFunction
    {
        [FunctionName("QueueTriggerCSharp")]        
        public async static Task Run([QueueTrigger("retryqueue", Connection = "AzureWebJobsStorage")]string url,
             [Table(tableName: "uploadlog", Connection = "AzureWebJobsStorage")]CloudTable uploadlogtable, 
             TraceWriter log)
        {
            log.Info($"Retry: {url}");

            try
            {


                // Primary Storage
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("AzureWebJobsStorage"));
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = blobClient.GetContainerReference("webroot");
                // await container.CreateIfNotExistsAsync();

                // Secondary Storage
                CloudStorageAccount secondaryStorageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("SecondStorage"));
                CloudBlobClient secondaryBlobClient = secondaryStorageAccount.CreateCloudBlobClient();
                CloudBlobContainer secondaryContainer = secondaryBlobClient.GetContainerReference("webroot");
                //await secondaryContainer.CreateIfNotExistsAsync();

                Uri uri = new Uri(url);
                string path = uri.AbsolutePath.Substring(1);

                using (HttpClient client = new HttpClient())
                {
                    // download
                    byte[] bytes = await client.GetByteArrayAsync(url);

                    // Copy to primary storage
                    CloudBlockBlob blockBlob = container.GetBlockBlobReference(path);
                    await blockBlob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);

                    // Copy to secondary storage
                    CloudBlockBlob secondaryBlockBlob = secondaryContainer.GetBlockBlobReference(path);
                    await secondaryBlockBlob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);
                }

                // Count completed items. 
                TableQuery<UploadLogEntry> uploaditemquery = new TableQuery<UploadLogEntry>().Where(TableQuery.GenerateFilterCondition("Url", QueryComparisons.Equal, url));
                var item = uploadlogtable.ExecuteQuery(uploaditemquery).FirstOrDefault();

                item.IsSuccess = true;

                TableOperation updateOperation = TableOperation.InsertOrMerge(item);
                uploadlogtable.Execute(updateOperation);
            }
            catch (Exception ex)
            {
                log.Info($"error: {ex}");
            }
        }
    }
}
