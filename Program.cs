using Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace todo {
    class Program {
        private const string EndpointUrl = "https://localhost:8081/";
        private const string AuthorizationKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private const string DatabaseId = "FamilyDatabase";
        private const string ContainerId = "FamilyContainer";
        static async Task Main(string[] args) {
            CosmosClient cosmosClient = new CosmosClient(EndpointUrl, AuthorizationKey);
            await Program.CreateDatabaseAsync(cosmosClient);
            await Program.CreateContainerAsync(cosmosClient);
            await Program.AddItemsToContainerAsync(cosmosClient);
            await Program.QueryItemsAsync(cosmosClient);
            await Program.ReplaceFamilyItemAsync(cosmosClient);
            await Program.DeleteFamilyItemAsync(cosmosClient);
            await Program.DeleteDatabaseAndCleanupAsync(cosmosClient);
            Console.WriteLine("Click any key to exit...");
            Console.ReadLine();
        }

        /// <summary>
        /// Create the database if it does not exist
        /// </summary>
        private static async Task CreateDatabaseAsync(CosmosClient cosmosClient) {
            CosmosDatabase database = await cosmosClient.CreateDatabaseIfNotExistsAsync(Program.DatabaseId);
            Console.WriteLine("Created Database: {0}\n", database.Id);
        }

        /// <summary>
        /// Create the container if it does not exist. 
        /// Specify "/LastName" as the partition key since we're storing family information, 
        /// to ensure good distribution of requests and storage.
        /// </summary>
        /// <returns></returns>
        private static async Task CreateContainerAsync(CosmosClient cosmosClient) {
            CosmosContainer container = await cosmosClient.GetDatabase(Program.DatabaseId).CreateContainerIfNotExistsAsync(Program.ContainerId, "/LastName");
            Console.WriteLine("Created Container: {0}\n", container.Id);
        }

        /// <summary>
        /// Add Family items to the container
        /// </summary>
        private static async Task AddItemsToContainerAsync(CosmosClient cosmosClient) {

            // Create a family object for the Adamski family.
            Family adamskiFamily = new Family {
                Id = "Adamski.1",
                LastName = "Adamski",
                Parents = new Parent[] {
                    new Parent { FirstName = "Victor" },
                    new Parent { FirstName = "Cynthia" }
                },
                Children = new Child[] {
                    new Child {
                        FirstName = "James Thomas",
                        Gender = "male",
                        Grade = 5,
                        Pets = new Pet[] {
                            new Pet { GivenName = "Snickers" }
                        }
                    }
                },
                Address = new Address { State = "IL", County = "Kane", City = "Carpentersville" },
                IsRegistered = false
            };


            CosmosContainer container = cosmosClient.GetContainer(Program.DatabaseId, Program.ContainerId);

            try {
                // Read the item to see if it exists.  
                ItemResponse<Family> adamskiFamilyResponse = await container.ReadItemAsync<Family>(adamskiFamily.Id, new PartitionKey(adamskiFamily.LastName));
                Console.WriteLine("Item in database with id: {0} already exists\n", adamskiFamilyResponse.Value.Id);
            } catch (CosmosException ex) when (ex.Status == (int)HttpStatusCode.NotFound) {

                // Create an item in the container representing the Adamski family. Note we provide the value of the partition key for this item, which is "Adamski"
                ItemResponse<Family> adamskiFamilyResponse = await container.CreateItemAsync<Family>(adamskiFamily, new PartitionKey(adamskiFamily.LastName));

                // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse.
                Console.WriteLine("Created item in database with id: {0}\n", adamskiFamilyResponse.Value.Id);
            }

            // Create a family object for the Orlando family.
            Family OrlandoFamily = new Family {
                Id = "Orlando.1983",
                LastName = "Orlando",
                Parents = new Parent[] {
                    new Parent { FamilyName = "Camel",   FirstName = "Nancy" },
                    new Parent { FamilyName = "Orlando", FirstName = "Mark" }
                },
                Children = new Child[] {
                    new Child {
                        FamilyName = "Orlando",
                        FirstName = "Megan",
                        Gender = "female",
                        Grade = 8,
                        Pets = new Pet[] {
                            new Pet { GivenName = "Blue" },
                            new Pet { GivenName = "Max" }
                        }
                    },
                    new Child {
                        FamilyName = "Orlando",
                        FirstName = "Nicholas",
                        Gender = "male",
                        Grade = 1
                    }
                },
                Address = new Address { State = "IL", County = "DuPage", City = "Villa Park" },
                IsRegistered = true
            };

            // Create an item in the container representing the Orlando family. Note we provide the value of the partition key for this item, which is "Orlando"
            ItemResponse<Family> orlandoFamilyResponse = await container.UpsertItemAsync<Family>(OrlandoFamily, new PartitionKey(OrlandoFamily.LastName));

            // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
            Console.WriteLine("Created item in database with id: {0}\n", orlandoFamilyResponse.Value.Id);
        }

        /// <summary>
        /// Run a query (using Azure Cosmos DB SQL syntax) against the container
        /// </summary>
        private static async Task QueryItemsAsync(CosmosClient cosmosClient) {
            var sqlQueryText = "SELECT * FROM c WHERE c.LastName = 'Adamski'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            CosmosContainer container = cosmosClient.GetContainer(Program.DatabaseId, Program.ContainerId);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

            List<Family> families = new List<Family>();

            await foreach (Family family in container.GetItemQueryIterator<Family>(queryDefinition)) {
                families.Add(family);
                Console.WriteLine("\tRead {0}\n", family);
            }
        }

        /// <summary>
        /// Replace an item in the container
        /// </summary>
        private static async Task ReplaceFamilyItemAsync(CosmosClient cosmosClient) {
            CosmosContainer container = cosmosClient.GetContainer(Program.DatabaseId, Program.ContainerId);

            ItemResponse<Family> orlandoFamilyResponse = await container.ReadItemAsync<Family>("Orlando.1983", new PartitionKey("Orlando"));
            Family itemBody = orlandoFamilyResponse;

            // update registration status from false to true
            itemBody.IsRegistered = true;
            // update grade of child
            itemBody.Children[0].Grade = 6;

            // replace the item with the updated content
            orlandoFamilyResponse = await container.ReplaceItemAsync<Family>(itemBody, itemBody.Id, new PartitionKey(itemBody.LastName));
            Console.WriteLine("Updated Family [{0},{1}].\n \tBody is now: {2}\n", itemBody.LastName, itemBody.Id, orlandoFamilyResponse.Value);
        }

        /// <summary>
        /// Delete an item in the container
        /// </summary>
        private static async Task DeleteFamilyItemAsync(CosmosClient cosmosClient) {
            CosmosContainer container = cosmosClient.GetContainer(Program.DatabaseId, Program.ContainerId);

            string partitionKeyValue = "Orlando";
            string familyId = "Orlando.1983";

            // Delete an item. Note we must provide the partition key value and id of the item to delete
            ItemResponse<Family> orlandoFamilyResponse = await container.DeleteItemAsync<Family>(familyId, new PartitionKey(partitionKeyValue));
            Console.WriteLine("Deleted Family [{0},{1}]\n", partitionKeyValue, familyId);
        }

        /// <summary>
        /// Delete the database and dispose of the Cosmos Client instance
        /// </summary>
        private static async Task DeleteDatabaseAndCleanupAsync(CosmosClient cosmosClient) {
            CosmosDatabase database = cosmosClient.GetDatabase(Program.DatabaseId);
            DatabaseResponse databaseResourceResponse = await database.DeleteAsync();

            Console.WriteLine("Deleted Database: {0}\n", Program.DatabaseId);
        }
    }
}
