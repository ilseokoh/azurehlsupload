using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using AzureHLSUploader.Models;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Azure.Management.Cdn;
using Microsoft.Azure;
using M3u8Parser.Utils;

namespace AzureHLSUploader
{
    public static class ContentPreloadFunction
    {
        // triggered every minute
        [FunctionName("ContentPreload")]
        public async static Task Run([TimerTrigger("0 * * * * *")]TimerInfo myTimer,
                                [Table(tableName: "preloadlog", Connection = "AzureWebJobsStorage")]CloudTable logtable,
                                [Queue(queueName: "preloadqueue", Connection = "AzureWebJobsStorage")]CloudQueue preloadqueue,
                                [Table(tableName: "m3u8log", Connection = "AzureWebJobsStorage")]CloudTable rootlogtable,
                                TraceWriter log)
        {
            log.Info($"------ Preload function executed at: {DateTime.Now}");

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

            // gathering items. preload needs path only
            var pathitems = new List<string>();
            foreach(var url in uploaditem.Items)
            {
                Uri uri = new Uri(url);
                pathitems.Add(uri.AbsolutePath);
            }

            // create log table
            await logtable.CreateIfNotExistsAsync();

            // Log first
            PreloadLogEntry entrylog = new PreloadLogEntry(uploaditem.Url, pathitems.Count);
            TableOperation insertOperation = TableOperation.InsertOrMerge(entrylog);
            logtable.Execute(insertOperation);

            try
            {
                // Request preloading 
                CdnManagementClient cdn = new CdnManagementClient(new TokenCredentials(GetAuthorizationToken()))
                {
                    SubscriptionId = CloudConfigurationManager.GetSetting("SubscriptionID")
                };

                cdn.Endpoints.LoadContent("OriginShieldGroup", "ODKOriginShield", "odkoriginshield", pathitems);
            }
            catch(Exception ex)
            {
                log.Info($"***** Fail to preload: {ex.Message}");

                await QueuePreloadItems(uploaditem, preloadqueue);
            }

            // complete
            entrylog.IsPreloadComplete = true;
            logtable.Execute(insertOperation);

            // 
            // Check result and log to root table log 
            TableQuery<M3u8PaserLogEntry> entryquery = new TableQuery<M3u8PaserLogEntry>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, EscapeTablekey.Replace(uploaditem.Url)));
            var m3u8entrylog = rootlogtable.ExecuteQuery(entryquery).FirstOrDefault();

            // Count completed items. 
            TableQuery<M3u8PaserLogEntry> countquery = new TableQuery<M3u8PaserLogEntry>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, EscapeTablekey.Replace(uploaditem.Url)),
                    TableOperators.And,
                    TableQuery.GenerateFilterConditionForBool("IsPreloadComplete", QueryComparisons.Equal, true))
               );

            var uploadcount = logtable.ExecuteQuery(countquery).Count();

            if (m3u8entrylog == null) throw new InvalidOperationException("there is no m3u8 entry log on the table.");
            m3u8entrylog.PreloadedTsCount = uploadcount;

            TableOperation updateOperation = TableOperation.InsertOrMerge(m3u8entrylog);
            rootlogtable.Execute(updateOperation);
        }

        private async static Task QueuePreloadItems(UploadItem item, CloudQueue preloadqueue)
        {
            await preloadqueue.CreateIfNotExistsAsync();

            CloudQueueMessage message = new CloudQueueMessage(JsonConvert.SerializeObject(item));
            // retry 3 times with 1 sec delay
            await RetryHelper.RetryOnExceptionAsync(3, TimeSpan.FromSeconds(1), async () =>
            {
                await preloadqueue.AddMessageAsync(message);
            });
        }

        private static string GetAuthorizationToken()
        {
            string clientId = CloudConfigurationManager.GetSetting("ClientId");
            string clientSecret = CloudConfigurationManager.GetSetting("ClientSecret");
            string tenantId = CloudConfigurationManager.GetSetting("TenantId");

            ClientCredential cc = new ClientCredential(clientId, clientSecret);
            var context = new AuthenticationContext("https://login.windows.net/" + tenantId);
            var result = context.AcquireTokenAsync("https://management.azure.com/", cc);
            if (result == null)
            {
                throw new InvalidOperationException("Failed to obtain the JWT token");
            }

            return result.Result.AccessToken;
        }
        
    }

    public class PreloadLogEntry : TableEntity
    {
        public PreloadLogEntry(string rooturl, int count)
        {
            this.PartitionKey = "preload";
            this.RowKey = EscapeTablekey.Replace(rooturl);
            this.CompleteCount = count;
            IsPreloadComplete = false;
        }

        public bool IsPreloadComplete { get; set; }

        public int CompleteCount { get; set; }
    }
}