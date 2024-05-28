using Nest;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;

namespace KafkaRepublish_Utility.Helper
{
    public class ElasticHelper
    {
        private readonly string elasticUrl;
        private readonly string index;
        private readonly string apiKeyId;
        private readonly String apiKey;
        List<object> allMessages = new List<object>();
        public ElasticHelper() 
        {
            elasticUrl = ConfigurationManager.AppSettings.Get("ElkURL");
            index = ConfigurationManager.AppSettings.Get("Index");
            apiKeyId = ConfigurationManager.AppSettings.Get("ApiKeyId");
            apiKey = ConfigurationManager.AppSettings.Get("ApiKey");
        }

        public async Task<List<object>> Fetchlogs(string fromDate, string toDate)
        {
            var settings = new ConnectionSettings(new Uri(elasticUrl))
                .DefaultIndex(index)
                .ApiKeyAuthentication(apiKeyId, apiKey);
            var client = new ElasticClient(settings);

            var searchResponse = await client.SearchAsync<dynamic>(s => s
            .Query(q => q
                    .Bool(b => b
                        .Must(mu => mu
                        .Term(t => t
                        .Field("data_stream.dataset")
                        .Value("intellicore")
                        ),
                        mu => mu
                        .Term(t => t
                        .Field("data_stream.namespace")
                        .Value("production")
                        ),
                        mu => mu
                        .MatchPhrase(m => m
                        .Field("message")
                        .Query("*Error publishing message to Kafka topic*")
                        ),
                        mu => mu
                        .DateRange(r => r
                        .Field("@timestamp")
                        .GreaterThan(fromDate)
                        .LessThan(toDate)))
                        )
                  )
                .Size(100)
            );
            if (searchResponse.IsValid)
            {
                foreach (var document in searchResponse.Documents)
                {
                    allMessages.Add(document["message"]);
                }
            }
            else
            {
                Console.WriteLine($"Error occurred: {searchResponse.OriginalException.Message}");
            }
            return allMessages;
        }
    }
}
