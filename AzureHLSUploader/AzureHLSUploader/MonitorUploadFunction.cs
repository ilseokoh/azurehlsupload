using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using Newtonsoft.Json;
using System;

namespace AzureHLSUploader
{
    public static class MonitorUploadFunction
    {
        private static TableQuery entryquery;

        [FunctionName("MonitorUpload")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "status")]HttpRequestMessage req,
                                                            [Table(tableName: "uploadlog", Connection = "AzureWebJobsStorage")]CloudTable logtable,
                                                            [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable rootlogtable,
                                                            TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");

            // Check result and log to root table log 
            //TableQuery<M3u8PaserLogEntry> progressquery = new TableQuery<M3u8PaserLogEntry>().Where(TableQuery.GenerateFilterConditionForBool("IsUploadComplete", QueryComparisons.Equal, false));
            //var ongoinglogs = rootlogtable.ExecuteQuery(progressquery).ToList();

            //TableQuery<M3u8PaserLogEntry> errorquery = new TableQuery<M3u8PaserLogEntry>().Where(TableQuery.GenerateFilterConditionForBool("HasError", QueryComparisons.Equal, true));
            //var errorlogs = rootlogtable.ExecuteQuery(errorquery).ToList();
            try
            {
                TableQuery<M3u8PaserLogEntry> totalquery = new TableQuery<M3u8PaserLogEntry>();
                var m3u8logs = rootlogtable.ExecuteQuery(totalquery).ToList();

                var ongoingLogs = m3u8logs.Where(x => x.IsUploadComplete == false).ToList();
                var errorLogs = m3u8logs.Where(x => x.HasError == true).ToList();

                var uploadStatus = new uploadStatus
                {
                    totalCount = m3u8logs.Count(),
                    ongoingCount = ongoingLogs.Count(),
                    errorCount = errorLogs.Count(),
                    ongoingList = ongoingLogs.Select(x => new itemStatus
                    {
                        url = x.Url,
                        fileCount = x.TsCount,
                        completeCount = x.UploadedTsCount,
                        hasError = x.HasError,
                        progress = (double)(x.UploadedTsCount / x.TsCount)
                    }).ToList(),
                    errorList = errorLogs.Select(x => new itemStatus
                    {
                        url = x.Url,
                        fileCount = x.TsCount,
                        completeCount = x.UploadedTsCount,
                        hasError = x.HasError,
                        progress = (double)(x.UploadedTsCount / x.TsCount)
                    }).ToList()
                };

                return req.CreateResponse(HttpStatusCode.OK, JsonConvert.SerializeObject(uploadStatus));
            }
            catch(Exception ex)
            {
                return req.CreateResponse(HttpStatusCode.InternalServerError, ex.Message);
            }
        }
    }

    public class uploadStatus
    {
        public int totalCount { get; set; }

        public int errorCount { get; set; }

        public int ongoingCount { get; set; }

        public List<itemStatus> ongoingList { get; set; }

        public List<itemStatus> errorList { get; set; }
    }

    public class itemStatus
    {
        public string url { get; set; }

        public int fileCount { get; set; }

        public int completeCount { get; set; }

        public double progress { get; set; }

        public bool hasError { get; set; }
    }
}