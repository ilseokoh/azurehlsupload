using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using M3u8Parser.Utils;
using System.Linq;
using Microsoft.Azure.WebJobs.Extensions.Http;
using System.Net.Http;
using System.Net;
using System.Threading.Tasks;

namespace AzureHLSErrorCollector
{
    public static class ErrorCollectorFunction
    {
        [FunctionName("ErrorCollector")]
        public static async Task Run([TimerTrigger("0 */2 * * * *")]TimerInfo myTimer,
            [Queue(queueName: "retryqueue", Connection = "AzureWebJobsStorage")]CloudQueue retryqueue, 
            TraceWriter log)
        {
            log.Info($"Collect error log");

            var m = await retryqueue.GetMessageAsync();
            if (m != null)
            {
                log.Info($"already has task. exit");
            }

            // Primary Storage
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("AzureWebJobsStorage"));
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("uploadlog");

            // Count completed items. 
            TableQuery<UploadLogEntry> uploaditemquery = new TableQuery<UploadLogEntry>().Where(TableQuery.GenerateFilterConditionForBool("IsSuccess", QueryComparisons.Equal, false));
            var items = table.ExecuteQuery(uploaditemquery).ToList();

            foreach(var item in items)
            {
                CloudQueueMessage message = new CloudQueueMessage(item.Url);
                await retryqueue.AddMessageAsync(message);
            }

            log.Info("Done Count: " + items.Count);

        }
    }

    public class UploadLogEntry : TableEntity
    {
        public UploadLogEntry(string rooturl, string url)
        {
            this.PartitionKey = EscapeTablekey.Replace(rooturl);
            this.RowKey = EscapeTablekey.Replace(url);

            this.Url = url;

            IsSuccess = false;
            ErrorMessage = "";
            Duration = 0;
        }

        public string Url { get; set; }

        public UploadLogEntry() { }

        public bool IsSuccess { get; set; }

        public double Duration { get; set; }

        public string ErrorMessage { get; set; }
    }
}