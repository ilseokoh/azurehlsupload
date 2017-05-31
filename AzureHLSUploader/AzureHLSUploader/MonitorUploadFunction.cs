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
using System.Net.Http.Formatting;

namespace AzureHLSUploader
{
    public static class MonitorUploadFunction
    {
        [FunctionName("MonitorUpload")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "monitor")]HttpRequestMessage req,
                                                            [Table(tableName: "uploadlog", Connection = "AzureWebJobsStorage")]CloudTable logtable,
                                                            [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable rootlogtable,
                                                            TraceWriter log)
        {
            log.Info("Monitoring Upload.");

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
                        fileCount = x.TotlaFileCount,
                        completeCount = x.UploadedCount,
                        hasError = x.HasError,
                        progress = (x.TotlaFileCount == 0 ? 0 : ((decimal)x.UploadedCount / (decimal)x.TotlaFileCount))
                    }).ToList(),
                    errorList = errorLogs.Select(x => new itemStatus
                    {
                        url = x.Url,
                        fileCount = x.TotlaFileCount,
                        completeCount = x.UploadedCount,
                        hasError = x.HasError,
                        progress = (x.TotlaFileCount == 0 ? 0 : ((decimal)x.UploadedCount / (decimal)x.TotlaFileCount))
                    }).ToList()
                };

                return req.CreateResponse(HttpStatusCode.OK, uploadStatus, JsonMediaTypeFormatter.DefaultMediaType);
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

        public decimal progress { get; set; }

        public bool hasError { get; set; }
    }
}