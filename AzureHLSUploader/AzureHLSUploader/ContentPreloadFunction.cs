using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Queue;

namespace AzureHLSUploader
{
    public static class ContentPreloadFunction
    {
        // triggered every minute
        [FunctionName("ContentPreload")]
        public static void Run([TimerTrigger("0 * * * * *")]TimerInfo myTimer,
                                [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable logtable,
                                [Queue(queueName: "preloadqueue", Connection = "AzureWebJobsStorage")]CloudQueue preloadqueue,
                                TraceWriter log)
        {
            log.Info($"------ Preload function executed at: {DateTime.Now}");

            // condition 
            // 1) content upload complet
            // 2) 
        }
    }
}