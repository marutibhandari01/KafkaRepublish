using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net.Http;
using System.Threading.Tasks;

namespace KafkaRepublish_Utility.Helper
{
    public class SumoHelper
    {
        private readonly string SumouserId;
        private readonly string SumouserKey;
        List<object> allMessages = new List<object>();

        public SumoHelper() 
        {
            SumouserId = ConfigurationManager.AppSettings.Get("SumoUserId");
            SumouserKey = ConfigurationManager.AppSettings.Get("SumoUserKey");
        }

        public async Task<List<object>> Fetchlogs(string fromDate, string toDate)
        {
            using (HttpClient client = new HttpClient())
            {
                string apiUrl = "https://api.us2.sumologic.com/api/v1/search/jobs";
                string query = "_source = \"intellicore-service\" AND NOT _sourceCategory=dev/intellicore/intellicore-service/facility/* \"Error publishing message to Kafka topic\"";
                int offset = 0;
                int limit = 200;

                client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                    "Basic", Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes($"{SumouserId}:{SumouserKey}")));

                // Creating search job
                var queryParameters = new
                {
                    query = query,
                    from = fromDate,
                    to = toDate,
                    timeZone = "CST",
                    byReceiptTime = true
                };

                HttpResponseMessage response = client.PostAsJsonAsync(apiUrl, queryParameters).Result;
                response.EnsureSuccessStatusCode();

                var jobLink = (await response.Content.ReadAsAsync<dynamic>())["link"]["href"].ToString();

                // Checking job status
                bool messageCountSet = false;
                int counter = 0;
                int messageCount = 0;

                while (!messageCountSet)
                {
                    response = client.GetAsync(jobLink).Result;
                    response.EnsureSuccessStatusCode();

                    dynamic jobStatus = response.Content.ReadAsAsync<dynamic>().Result;
                    Logger.LogMessage($"Status Code: {response.StatusCode}");
                    Logger.LogMessage($"Message found: {jobStatus.messageCount}");

                    counter++;

                    if (jobStatus.state == "DONE GATHERING RESULTS")
                    {
                        messageCount = jobStatus.messageCount;
                        messageCountSet = true;
                        break;
                    }
                    else
                    {
                        await Task.Delay(10000); // Wait for 10 seconds before checking job status again
                    }

                    if (counter > 10)
                    {
                        Logger.LogMessage("Breaking job has not finished after 10 checks");
                        Logger.LogMessage($"Last response: {jobStatus}");
                        return null;
                    }
                }

                while (offset < messageCount)
                {
                    Logger.LogMessage($"Querying messages offset {offset} limit {limit}");
                    response = client.GetAsync($"{jobLink}/messages?offset={offset}&limit={limit}").Result;
                    response.EnsureSuccessStatusCode();

                    dynamic messages = response.Content.ReadAsAsync<dynamic>().Result;
                    Logger.LogMessage($"Status Code: {response.StatusCode}");

                    allMessages.AddRange(messages["messages"]);
                    offset += limit;

                    await Task.Delay(5000); // Wait for 5 seconds before making the next request
                }

            }
            return allMessages;
        }
    }
}
