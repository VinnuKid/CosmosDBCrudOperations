using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System.Linq;

namespace CosmosDB
{
    public static class CosmosDbOperations
    {

        private static readonly string EndpointUri = Environment.GetEnvironmentVariable("EndpointUri");
        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = Environment.GetEnvironmentVariable("PrimaryKey");
        private static readonly string databaseId = Environment.GetEnvironmentVariable("databaseId");
        private static readonly string collectionId = Environment.GetEnvironmentVariable("collectionId");





        [FunctionName("CosmosDbOperations")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {

            var outputs = new List<string>();

            var family = context.GetInput<Family>();
            //outputs.Add(await context.CallActivityAsync<string>("ListDatabases", null));
            //outputs.Add(await context.CallActivityAsync<string>("CreateDatabase", null));
            //outputs.Add(await context.CallActivityAsync<string>("ListDatabases", null));
            //outputs.Add(await context.CallActivityAsync<string>("DeleteDatabase", databaseId));
            //outputs.Add(await context.CallActivityAsync<string>("ListDatabases", null));

            outputs.Add(await context.CallActivityAsync<string>("ListCollections", databaseId));
            outputs.Add(await context.CallActivityAsync<string>("CreateCollection", databaseId));
            outputs.Add(await context.CallActivityAsync<string>("ListCollections", databaseId));
            //outputs.Add(await context.CallActivityAsync<string>("DeleteCollection", databaseId));
            //outputs.Add(await context.CallActivityAsync<string>("ListCollections", databaseId));

            return outputs;
        }



        [FunctionName("ListDatabases")]
        public static void ListDatabases([ActivityTrigger] DurableActivityContext context, ILogger log)
        {

            using (var client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {
                // ToList only recommended for databases where we have less number of return count, but not suggestable with documents
                var databaseList = client.CreateDatabaseQuery().ToList();

                foreach (var database in databaseList)
                {
                    DatabaseDetails(database);
                }


            }
        }

     
        [FunctionName("CreateDatabase")]
        public static async Task CreateDatabase([ActivityTrigger] DurableActivityContext context, ILogger log)
        {

            using (var client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {
                Database database = new Database()
                {
                    Id = databaseId
                };

                var databaseResponse = await client.CreateDatabaseIfNotExistsAsync(database);
                DatabaseDetails(databaseResponse.Resource);
            }
        }



        [FunctionName("DeleteDatabase")]
        public static async Task DeleteDatabase([ActivityTrigger] DurableActivityContext context, ILogger log)
        {

            string DatabaseId = context.GetInput<string>();
            using (var client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {
                var databaseUri = UriFactory.CreateDatabaseUri(DatabaseId);
                //var db=  client.CreateDatabaseQuery().SingleOrDefault(database => database.Id == DatabaseId);

                var databaseResponse = await client.DeleteDatabaseAsync(databaseUri);

            }
        }



        [FunctionName("ListCollections")]
        public static void ListCollections([ActivityTrigger] DurableActivityContext context, ILogger log)
        {

            using (var client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {

                var databaseUri = UriFactory.CreateDatabaseUri(context.GetInput<string>());
                // ToList only recommended for databases where we have less number of return count, but not suggestable with documents
                var CollectionList = client.CreateDocumentCollectionQuery(databaseUri).ToList();

                foreach (var Collection in CollectionList)
                {
                    ViewCollectionDetails(Collection);
                }


            }
        }

        [FunctionName("CreateCollection")]
        public static async Task CreateCollection([ActivityTrigger] DurableActivityContext context, ILogger log)
        {

            using (var client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {

                var databaseUri = UriFactory.CreateDatabaseUri(context.GetInput<string>());
                DocumentCollection collectionDefinition = new DocumentCollection();
                collectionDefinition.Id = collectionId;
                collectionDefinition.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });
                collectionDefinition.PartitionKey.Paths.Add("/LastName");

                RequestOptions requestOptions = new RequestOptions()
                {
                    OfferThroughput = 500
                };

                var response = await client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, collectionDefinition, requestOptions);
                ViewCollectionDetails(response.Resource);

            }
        }



        [FunctionName("DeleteCollection")]
        public static async Task DeleteCollection([ActivityTrigger] DurableActivityContext context, ILogger log)
        {

            string DatabaseId = context.GetInput<string>();
            using (var client = new DocumentClient(new Uri(EndpointUri), PrimaryKey))
            {
                var databaseUri = UriFactory.CreateDatabaseUri(DatabaseId);
                var collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
                var databaseResponse = await client.DeleteDocumentCollectionAsync(collectionUri);

            }
        }


        [FunctionName("CosmosDbOperations_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            ILogger log)
        {


            var family = await req.Content.ReadAsAsync<Family>();

            string instanceId = await starter.StartNewAsync("CosmosDbOperations", family);

            return starter.CreateCheckStatusResponse(req, instanceId);
        }


        private static void ViewCollectionDetails(DocumentCollection Resource)
        {
            Console.WriteLine("\tCollection: {0}\n", Resource.Id);
            Console.WriteLine();
            Console.WriteLine("\t\t\tCollection: {0}\n", Resource.ResourceId);
            Console.WriteLine("\t\t\tCollection: {0}\n", Resource.Timestamp);
            Console.WriteLine("\t\t\tCollection: {0}\n", Resource.SelfLink);
            Console.WriteLine("\t\t\tCollection: {0}\n", Resource.ETag);
        }

        private static void DatabaseDetails(Database database)
        {
            Console.WriteLine("\t Database Details: {0}\n", database.Id);
            Console.WriteLine();
            Console.WriteLine("\t\t\tCollection: {0}\n", database.ResourceId);
            Console.WriteLine("\t\t\tCollection: {0}\n", database.Timestamp);
            Console.WriteLine("\t\t\tCollection: {0}\n", database.SelfLink);
            Console.WriteLine("\t\t\tCollection: {0}\n", database.ETag);
        }


    }
}