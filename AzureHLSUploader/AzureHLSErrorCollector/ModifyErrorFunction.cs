using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Table;
using M3u8Parser.Utils;

namespace AzureHLSErrorCollector
{
    public static class ModifyErrorFunction
    {
        [FunctionName("CorrectError")]
        //public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "correct")]HttpRequestMessage req,
        public static async Task Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer,
            [Table(tableName: "uploadlog", Connection = "AzureWebJobsStorage")]CloudTable logtable,
            [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable rootlogtable,
            TraceWriter log)
        {
            log.Info("Check Upload progress");


            // Check result and log to root table log 
            TableQuery<M3u8PaserLogEntry> errorlistquery = new TableQuery<M3u8PaserLogEntry>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterConditionForBool("HasError", QueryComparisons.Equal, true),
                    TableOperators.Or,
                    TableQuery.GenerateFilterConditionForBool("IsUploadComplete", QueryComparisons.Equal, false))
                );

            var items = rootlogtable.ExecuteQuery(errorlistquery).ToList(); 

            foreach(var m3u8entrylog in items)
            {
                // Count completed items. 
                TableQuery<UploadLogEntry> uploaditemquery = new TableQuery<UploadLogEntry>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, EscapeTablekey.Replace(m3u8entrylog.Url)));

                var uploadcount = logtable.ExecuteQuery(uploaditemquery).Where(x => x.IsSuccess == true).Count();
                var errorcount = logtable.ExecuteQuery(uploaditemquery).Where(x => x.IsSuccess == false).Count();

                m3u8entrylog.UploadedCount = uploadcount;
                // check upload complete
                if (m3u8entrylog.TotlaFileCount == uploadcount) m3u8entrylog.IsUploadComplete = true;
                // check error count
                if (errorcount > 0) m3u8entrylog.HasError = true;
                else m3u8entrylog.HasError = false;

                TableOperation updateOperation = TableOperation.InsertOrMerge(m3u8entrylog);
                await rootlogtable.ExecuteAsync(updateOperation);

                log.Info(m3u8entrylog.Url);
            }
        }
    }

    public class M3u8PaserLogEntry : TableEntity
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
    }
}