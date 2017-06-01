using System.Linq;
using System.Net;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Net.Http.Formatting;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureHLSUploader
{
    public static class RequestUploadFunction
    {
        [FunctionName("RequestUpload")]

        public async static Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = "upload")]HttpRequestMessage req,
                                                            [Queue(queueName: "m3u8queue", Connection = "AzureWebJobsStorage")]CloudQueue uploadqueue,
                                                            [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable uploadlogtable,
                                                            TraceWriter log)
        {
            var body = await req.Content.ReadAsStringAsync();
            try
            {
                var requestItem = JsonConvert.DeserializeObject<RequestItem>(body);

                log.Info($"request upload : {requestItem.primaryUrl}");
                var msg = new CloudQueueMessage(JsonConvert.SerializeObject(requestItem));

                await uploadqueue.AddMessageAsync(msg);

                M3u8PaserLogEntry entrylog = new M3u8PaserLogEntry(requestItem.primaryUrl);
                entrylog.OriginRequest = body;

                TableOperation insertOperation = TableOperation.InsertOrMerge(entrylog);
                uploadlogtable.Execute(insertOperation);

                var response = new ApiResponse
                {
                    status = "success",
                    data = "",
                    message = $"requested: {requestItem.primaryUrl}"
                };

                log.Info($"response : {JsonConvert.SerializeObject(response)}");

                // Fetching the name from the path parameter in the request URL
                return req.CreateResponse(HttpStatusCode.OK, response, JsonMediaTypeFormatter.DefaultMediaType);
            }
            catch(Exception ex)
            {
                var errresponse = new ApiResponse
                {
                    status = "error",
                    data = body,
                    message = ex.Message
                };

                return req.CreateResponse(HttpStatusCode.InternalServerError, errresponse, JsonMediaTypeFormatter.DefaultMediaType);
            }
        }
    }

    public class ApiResponse
    {
        public string status { get; set; }

        public object data { get; set; }

        public string message { get; set; }
    }

    public class RequestItem
    {
        public string primaryUrl { get; set; }

        public List<string> secondaryUrls { get; set; }
    }
}