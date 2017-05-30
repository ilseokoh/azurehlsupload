using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Threading.Tasks;
using AzureHLSUploader.Models;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Net;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Table;
using M3u8Parser.Utils;
using Microsoft.WindowsAzure.Storage.Queue;

namespace AzureHLSUploader
{
    public static class VerizonPreloadFunction
    {
        static string token = CloudConfigurationManager.GetSetting("VerizonConsoleKey");
        static string clientid = CloudConfigurationManager.GetSetting("VerizonConsoleID");
        static string cdnhostname = CloudConfigurationManager.GetSetting("VerizonCDNHostname");

        [FunctionName("VerizonPreload")]
        public async static Task Run([TimerTrigger("0 * * * * *")]TimerInfo myTimer,
                                [Table(tableName: "preloadlog", Connection = "AzureWebJobsStorage")]CloudTable logtable,
                                [Queue(queueName: "preloadqueue", Connection = "AzureWebJobsStorage")]CloudQueue preloadqueue,
                                [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable rootlogtable,
                                TraceWriter log)
        {
            log.Info($"------ Preload function executed at: {DateTime.Now}");

            // create log table
            await logtable.CreateIfNotExistsAsync();

            int count = 0;
            do
            {
                // Get item from queue. 
                var queueitem = await preloadqueue.GetMessageAsync();
                if (queueitem == null || string.IsNullOrEmpty(queueitem.AsString)) return;

                UploadItem uploaditem = null;
                try
                {
                    uploaditem = JsonConvert.DeserializeObject<UploadItem>(queueitem.AsString);
                }
                catch (Exception ex)
                {
                    throw new ArgumentException("Message from queue has error", ex);
                }

                // Delete from queue
                await preloadqueue.DeleteMessageAsync(queueitem);

                // Request
                foreach (var url in uploaditem.Items)
                {
                    Uri uri = new Uri(url);
                    string asset = "http://" + cdnhostname + uri.AbsolutePath;

                    await LoadAsset(asset, logtable, uploaditem.Url);

                    log.Info($"Preload requested : {uri.AbsolutePath}");
                    count += 1;
                }
            } while (count < 150);
            //} while (count < 700);

            log.Info($"------ Preload complete: {count}");
        }

        private async static Task LoadAsset(string asset, CloudTable logtable, string rooturl)
        {
            var content = new VerizonPreloadContent
            {
                MediaPath = asset,
                MediaType = "3"
            };

            Uri uri = new Uri("https://api.edgecast.com/v2/mcc/customers/" + clientid + "/edge/load");

            using (WebClient client = new WebClient())
            {
                client.Headers.Add("Authorization", "TOK: " + token);
                client.Headers.Add("Content-Type", "application/json");
                client.Headers.Add("Accept", "application/json");
                var bodyText = JsonConvert.SerializeObject(content);

                PreloadLogEntry entrylog = new PreloadLogEntry(rooturl, asset);

                try
                {
                    var result = client.UploadString(uri, "PUT" ,bodyText);

                    entrylog.IsSuccess = true;
                }
                catch (Exception ex)
                {
                    //handle the exception here
                    entrylog.IsSuccess = false;
                    entrylog.ErrorMessage = ex.ToString();
                }

                TableOperation updateOperation = TableOperation.InsertOrMerge(entrylog);
                await logtable.ExecuteAsync(updateOperation);
            }
        }
    }

    public class VerizonPreloadContent
    {
        public string MediaPath { get; set; }

        public string MediaType { get; set; }
    }

    public class PreloadLogEntry : TableEntity
    {
        public PreloadLogEntry(string rooturl, string url)
        {
            this.PartitionKey = EscapeTablekey.Replace(rooturl);
            this.RowKey = EscapeTablekey.Replace(url);

            this.Url = url;

            IsSuccess = false;
            ErrorMessage = "";
            Duration = 0;
        }

        public string Url { get; set; }

        public PreloadLogEntry() { }

        public bool IsSuccess { get; set; }

        public double Duration { get; set; }

        public string ErrorMessage { get; set; }
    }

}