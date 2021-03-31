using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Lambda.DynamoDBEvents;
using Newtonsoft.Json;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
// Amazon.DynamoDBv2.DataModel;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace DbTrigger
{

    public class CompanyItem
    {
        public string itemId;//primary key
        public string description;
        public int rating;
        public string type;
        public string company;
        public string lastInstanceOfWord;
        public string count;
        
    }
    public class Function
    {
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();
       
        public async Task<List<CompanyItem>> FunctionHandler(DynamoDBEvent input, ILambdaContext context)
        {          
            Table table = Table.LoadTable(client, "Assignment5");
            List<CompanyItem> items = new List<CompanyItem>();
            List<DynamoDBEvent.DynamodbStreamRecord> records = (List<DynamoDBEvent.DynamodbStreamRecord>)input.Records; 
            
            if (records.Count > 0)
            {
                DynamoDBEvent.DynamodbStreamRecord record = records[0];
                if (record.EventName.Equals("INSERT"))
                {

                    Document myDoc = Document.FromAttributeMap(record.Dynamodb.NewImage);
                    CompanyItem myItem = JsonConvert.DeserializeObject<CompanyItem>(myDoc.ToJson());

                    //one of attempts at grabbing the count and rating to divide and get the average but had a hard time
                    //pulling the data out and working with it tried scans and getItemRequest
                                      
                    //var tableName = "RatingsByType";


                    //var request2 = new GetItemRequest
                    //{
                    //    TableName = tableName,
                    //    Key = new Dictionary<string, AttributeValue>() { { "type", new AttributeValue { S = myItem.type } } },
                    //    // Optional parameters.
                    //    ProjectionExpression = "count, rating",
                    //    ConsistentRead = true
                    //};
                    //var response = client.GetItemAsync(request2);

                    //var num = myItem.rating / Convert.ToInt32(response);

                    var request = new UpdateItemRequest
                    {                       
                        TableName = "RatingsByType",
                        Key = new Dictionary<string, AttributeValue>
                        {

                            { "type", new AttributeValue { S = myItem.type } }

                        },
                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        {
                            {
                                "count",
                                new AttributeValueUpdate { Action = "ADD", Value = new AttributeValue { N = "1" } }
                            },

                            {
                                "rating",
                                new AttributeValueUpdate { Action = "ADD", Value = new AttributeValue { N = myItem.rating.ToString() } }
                            },
                        },
                    };
                    await client.UpdateItemAsync(request);
                }
            }
            return items;
        }
    } 
}
