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

namespace AzureHLSUploader
{
    public static class RequestUploadFunction
    {
        [FunctionName("RequestUpload")]

        public async static Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = "upload")]HttpRequestMessage req,
                                                            [Queue(queueName: "m3u8queue", Connection = "AzureWebJobsStorage")]CloudQueue uploadqueue,
                                                            TraceWriter log)
        {
            var body = await req.Content.ReadAsStringAsync();
            try
            {
                var requestItmes = JsonConvert.DeserializeObject<List<RequestItem>>(body);

                log.Info($"request upload : {requestItmes.Count}");

                foreach (var item in requestItmes)
                {
                    await uploadqueue.AddMessageAsync(new CloudQueueMessage(JsonConvert.SerializeObject(item)));
                }

                var response = new ApiResponse
                {
                    status = "success",
                    data = "",
                    message = $"requested {requestItmes.Count} content(s)"
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
        public string PrimaryUrl { get; set; }

        public List<string> SecondaryUrls { get; set; }
    }
}