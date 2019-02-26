using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Linq;
using Microsoft.Azure.Documents.Linq;

namespace CosmosDB
{
    public static class CosmosDBDocumentOperations
    {

        private static readonly string EndpointUri = Environment.GetEnvironmentVariable("EndpointUri");
        private static readonly string PrimaryKey = Environment.GetEnvironmentVariable("PrimaryKey");
        private static readonly string databaseId = Environment.GetEnvironmentVariable("databaseId");
        private static readonly string collectionId = Environment.GetEnvironmentVariable("collectionId");


        [FunctionName("CosmosDBDocumentOperations")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {
            var outputs = new List<string>();

            Family payload = context.GetInput<Family>();
            try
            {

                //await context.CallActivityAsync<string>("CreateDocument", payload);
                await context.CallActivityAsync<string>("ListDocuments", null);
                await context.CallActivityAsync<string>("ReplaceDocument", null);
                await context.CallActivityAsync<string>("ListDocuments", null);
                await context.CallActivityAsync<string>("DeleteDocument", null);
            }

            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return outputs;
        }

        [FunctionName("CreateDocument")]
        public static async Task<Document> CreateDocument([ActivityTrigger] DurableActivityContext context, ILogger log)
        {

            var family = context.GetInput<Family>();
            try
            {
                using (DocumentClient client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
                {
                    var documentCollectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
                    var response = await client.CreateDocumentAsync(documentCollectionUri, family);
                    DocumentDetails(response.Resource,databaseId);
                    return response;
                }
            }

            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return null;
            }

        }

        
        [FunctionName("ListDocuments")]
        public static void ListDocuments([ActivityTrigger] DurableActivityContext context, ILogger log)
        {

            var sql = "select * from c";

            using (DocumentClient client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {
                var documentCollectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
                FeedOptions feedOptions = new FeedOptions() { EnableCrossPartitionQuery = true };
                var documents = client.CreateDocumentQuery<Document>(documentCollectionUri, sql, feedOptions).ToList();

                foreach (var document in documents)
                {
                    DocumentDetails(document, databaseId);
                }

            }


        }

        [FunctionName("ListDocumentsWithWhereCondition")]
        public static  void ListDocumentsWithWhereCondition([ActivityTrigger] DurableActivityContext context, ILogger log)
        {

            var sql = $"select * from c where c.IsRegistered=false";

            using (DocumentClient client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {
                var documentCollectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
                FeedOptions feedOptions = new FeedOptions() { EnableCrossPartitionQuery = true };
                var documents = client.CreateDocumentQuery<Document>(documentCollectionUri, sql, feedOptions).ToList();

                foreach (var document in documents)
                {
                    DocumentDetails(document, databaseId);
                }

            }


        }



        [FunctionName("ListDocumentsWithPaging")]
        public static async void ListDocumentsWithPaging([ActivityTrigger] DurableActivityContext context, ILogger log)
        {

            var sql = $"select * from c where c.IsRegistered=true";

            using (DocumentClient client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {
                var documentCollectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
                FeedOptions feedOptions = new FeedOptions() { MaxItemCount = 1, EnableCrossPartitionQuery = true };
                var query = client.CreateDocumentQuery<Document>(documentCollectionUri, sql, feedOptions).AsDocumentQuery();

                while (query.HasMoreResults)
                {
                    var documents = await query.ExecuteNextAsync<Document>();
                    foreach (var document in documents)
                    {
                        DocumentDetails(document, databaseId);
                    }

                }

            }


        }




        [FunctionName("ReplaceDocument")]
        public static async void ReplaceDocument([ActivityTrigger] DurableActivityContext context, ILogger log)
        {


            using (DocumentClient client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {
                var documentCollectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);

                var sql = "SELECT VALUE COUNT(C) FROM C WHERE C.LastName='Andersenassda'";
                FeedOptions feedOptions = new FeedOptions() { EnableCrossPartitionQuery = true, MaxItemCount = 1 };
                var count = client.CreateDocumentQuery(documentCollectionUri, sql, feedOptions).AsEnumerable().First();
                Console.WriteLine($"Updating {count} Documents");
                sql = "select * from c where startswith(c.LastName,'AndersenaasA')=true";
                var documents = client.CreateDocumentQuery<Family>(documentCollectionUri, sql, feedOptions).ToList();

                foreach (var document in documents)
                {
                    document.IsRegistered = true;
                    var response = await client.ReplaceDocumentAsync(document.SelfLink, document);
                    var updatedRecord = response.Resource;
                    Console.WriteLine(updatedRecord);
                }

            }


        }



        [FunctionName("DeleteDocument")]
        public static async void DeleteDocument([ActivityTrigger] DurableActivityContext context, ILogger log)
        {


            using (DocumentClient client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {
                var documentCollectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);

                var sql = "SELECT VALUE COUNT(C) FROM C WHERE C.LastName='AndersenaasA'";
                FeedOptions feedOptions = new FeedOptions() { EnableCrossPartitionQuery = true, MaxItemCount = 1 };
                var count = client.CreateDocumentQuery(documentCollectionUri, sql, feedOptions).AsEnumerable().First();
                Console.WriteLine($"Deleting {count} Documents");
                sql = "select c._self,c.LastName from c where startswith(c.LastName,'AndersenaasA')=true";
                var documents = client.CreateDocumentQuery<Family>(documentCollectionUri, sql, feedOptions).ToList();

                foreach (var document in documents)
                {
                    Console.WriteLine(document.ResourceId);
                    RequestOptions requestOptions = new RequestOptions()
                    {
                        PartitionKey = new PartitionKey(document.LastName)
                    };

                    await client.DeleteDocumentAsync(document.SelfLink, requestOptions);


                }

            }


        }

        [FunctionName("CosmosDBDocumentOperations_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            ILogger log)
        {

            string jsonContent = req.Content.ReadAsStringAsync().Result;

            var family = JsonConvert.DeserializeObject<Family>(jsonContent);


            string instanceId = await starter.StartNewAsync("CosmosDBDocumentOperations", family);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        private static void DocumentDetails(Document Resource, string databaseId)
        {
            Console.WriteLine("\t Document Details in Database: {0}\n", databaseId);
            Console.WriteLine();
            Console.WriteLine("\t\t\tCollection: {0}\n", Resource);

        }

    }
}