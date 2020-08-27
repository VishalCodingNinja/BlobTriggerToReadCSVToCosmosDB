using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace FunctionAppToReadCSV
{
    public static class InsertCSVRecordsToCosmosDBWithBlobTrigger
    {
        private const string EndpointUrl = "https://cosmosdb-121.documents.azure.com:443/";
        private const string AuthorizationKey = "TNF2z6HwigDyrqDltYr2UAq2FmeZB0oHqUkkBfSkJ9QdkKnPduWMLF46KHIMZgfLQvt2AWnTWaIm9VHMuaMFww==";
        private const string DatabaseName = "bulk-insert-db";
        private const string ContainerName = "items";
        private const int ItemsToInsert = 1500001;

        [FunctionName("Function1")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation($"Function Started successfully");
            //< CreateClient >
            CosmosClient cosmosClient = new CosmosClient(EndpointUrl, AuthorizationKey, new CosmosClientOptions() { AllowBulkExecution = true });
            // </CreateClient>

            log.LogInformation($"CreateClient");
            // Create with a throughput of 50000 RU/s
            // Indexing Policy to exclude all attributes to maximize RU/s usage
            log.LogInformation("This will create a 50000 RU/s container, press any key to continue.");


            // <Initialize>
            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(InsertCSVRecordsToCosmosDBWithBlobTrigger.DatabaseName);

            await database.DefineContainer(InsertCSVRecordsToCosmosDBWithBlobTrigger.ContainerName, "/pk")
                    .WithIndexingPolicy()
                        .WithIndexingMode(IndexingMode.Consistent)
                        .WithIncludedPaths()
                            .Attach()
                        .WithExcludedPaths()
                            .Path("/*")
                            .Attach()
                    .Attach()
                .CreateAsync(50000);

            //  </ Initialize >

            try
            {

                log.LogInformation($"Finished in writing {ItemsToInsert} items");

                // Prepare items for insertion
                Console.WriteLine($"Preparing {ItemsToInsert} items to insert...");
                // <Operations>
                Dictionary<PartitionKey, Stream> itemsToInsert = new Dictionary<PartitionKey, Stream>(ItemsToInsert);
                foreach (CSVRow item in (await InsertCSVRecordsToCosmosDBWithBlobTrigger.ReadCSVFileFromBlobStorage(log)).ToList())
                {
                    MemoryStream stream = new MemoryStream();
                    await JsonSerializer.SerializeAsync(stream, item);
                    itemsToInsert.Add(new PartitionKey(item.pk), stream);
                }
                // </Operations>

                // Create the list of Tasks
                log.LogInformation($"Starting...");
                Stopwatch stopwatch = Stopwatch.StartNew();
                // <ConcurrentTasks>
                Container container = database.GetContainer(ContainerName);
                List<Task> tasks = new List<Task>(ItemsToInsert);
                foreach (KeyValuePair<PartitionKey, Stream> item in itemsToInsert)
                {
                    tasks.Add(container.CreateItemStreamAsync(item.Value, item.Key)
                        .ContinueWith((Task<ResponseMessage> task) =>
                        {
                            using (ResponseMessage response = task.Result)
                            {
                                if (!response.IsSuccessStatusCode)
                                {
                                    Console.WriteLine($"Received {response.StatusCode} ({response.ErrorMessage}).");
                                }
                            }
                        }));
                }

                // Wait until all are done
                await Task.WhenAll(tasks);
                // </ConcurrentTasks>
                stopwatch.Stop();

                log.LogInformation($"Finished in writing {ItemsToInsert} items in {stopwatch.Elapsed}.");
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message);
            }
            finally
            {
                log.LogInformation("Cleaning up resources...");
                //await database.DeleteAsync();
            }

            return new OkObjectResult(new
            {
                Status = "Success",
                NoOfRecordsInserted = ItemsToInsert,
                FileName = "csv"
            });
        }

        public async static Task<string> ReadBlob(string BlobName)
        {
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=storageaccount90901;AccountKey=HM1DRs1IWPoETPXgqbdz950VRLYygxW6/zUQUgUDBxvN5kmwjQ3AuKrmT2Fs1YOnogu461wLVRTtwZnREtGm7w==;EndpointSuffix=core.windows.net");
            CloudBlobClient cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer CSVBlobContainer = cloudBlobClient.GetContainerReference("documentdata");
            CloudBlockBlob cloudBlockBlob = CSVBlobContainer.GetBlockBlobReference(BlobName);
            string fileContent;
            using (var memoryStream = new MemoryStream())
            {
                await cloudBlockBlob.DownloadToStreamAsync(memoryStream);
                fileContent = System.Text.Encoding.UTF8.GetString(memoryStream.ToArray());
            }
            return fileContent;
        }

        public static CSVRow[] ReadItemData(string employeesListContent)
        {
            List<CSVRow> itemListResult = new List<CSVRow>();
            var itemList = employeesListContent.Split(Environment.NewLine);

            for (int itemIndex = 0; itemIndex < itemList.Length; itemIndex++)
            {
                var itemVal = itemList[itemIndex];
                if (!string.IsNullOrEmpty(itemVal) && !string.IsNullOrWhiteSpace(itemVal))
                {
                    var partitionKey = Guid.NewGuid().ToString();
                    CSVRow item = null;
                    if (itemIndex == 0)
                    {
                        item = new CSVRow
                        {
                            id = partitionKey,
                            pk = partitionKey,
                            Value = itemVal
                        };
                    }
                    else
                    {
                        item = new CSVRow
                        {
                            id = partitionKey,
                            pk = partitionKey,
                            Value = itemVal
                        };
                    }
                    itemListResult.Add(item);
                }
            }
            return itemListResult.ToArray();
        }

        public static async Task<CSVRow[]> ReadCSVFileFromBlobStorage(ILogger log)
        {
            log.LogInformation("ReadCSV_AT Started");
            log.LogInformation("Reading the blob Started");
            var itemContent = await ReadBlob("1500000 Sales Records.csv");
            log.LogInformation("Reading the blob has Completed");
            log.LogInformation("Reading the CSV Data Started");
            var items = ReadItemData(itemContent);
            log.LogInformation("Reading the blob has Completed");
            log.LogInformation("ReadCSV_AT End");
            return items;
        }

#pragma warning disable S101 // Types should be named in PascalCase
        public class CSVRow
#pragma warning restore S101 // Types should be named in PascalCase
        {
            public string id { get; set; }
            public string pk { get; set; }
            public string Value { get; set; }
        }
    }
}
