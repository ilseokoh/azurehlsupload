using System.Linq;
using System.Net;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System.Threading.Tasks;
using System;
using Microsoft.WindowsAzure.Storage.Table;
using M3u8Parser.Utils;
using Newtonsoft.Json;
using System.Net.Http.Formatting;

namespace AzureHLSUploader
{
    public static class UploadStatusFunction
    {
        [FunctionName("UploadStatus")]

        public async static Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = "status")]HttpRequestMessage req,
                                                            [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable rootlogtable, 
                                                            TraceWriter log)
        {
            var url = await req.Content.ReadAsStringAsync();
            log.Info("request status: {url}");

            try
            {
                if (string.IsNullOrEmpty(url)) throw new ArgumentException("url is null");
                if (!url.ToLower().EndsWith(".m3u8")) throw new ArgumentException("url must be m3u8.");

                // Check result and log to root table log 
                TableQuery<M3u8PaserLogEntry> statusquery = new TableQuery<M3u8PaserLogEntry>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, EscapeTablekey.Replace(url)));
                var status = rootlogtable.ExecuteQuery(statusquery).FirstOrDefault();

                var statusItem = new itemStatus
                {
                    url = status.Url,
                    fileCount = status.TotlaFileCount,
                    completeCount = status.UploadedTsCount,
                    hasError = status.HasError,
                };

                if (status.TotlaFileCount == 0) statusItem.progress = 0;
                else statusItem.progress = ((decimal)status.UploadedTsCount / (decimal)status.TotlaFileCount);

                return req.CreateResponse(HttpStatusCode.OK, statusItem, JsonMediaTypeFormatter.DefaultMediaType);
            }
            catch (Exception ex)
            {
                return req.CreateResponse(HttpStatusCode.InternalServerError, ex.Message);
            }
        }
    }
}