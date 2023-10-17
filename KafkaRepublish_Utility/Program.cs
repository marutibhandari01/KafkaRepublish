using KafkaRepublish_Utility.Helper;
using KafkaRepublish_Utility.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading;

namespace KafkaRepublish_Utility
{
    public class Program
    {
        private static string topic = System.Configuration.ConfigurationManager.AppSettings.Get("Kafka_Topic");
        private const string SUBJECT_PACKAGEINDUCT = "PACKAGE.INITIALINDUCT";
        static void Main(string[] args)
        {
            string fromDate;
            string toDate;

            KafkaProducer producer = new KafkaProducer();
            producer.Setup();
            SumoHelper sumoHelper = new SumoHelper();
            List<object> logmessages = new List<object>();
            /* 
             * Arguments that can be supplied:
             * 'FromDate' of format yyyy/MM/dd
             * 'ToDate' of format yyyy/MM/dd
             * 'number of minutes'
            */

            if (args.Length < 1)
            {
                throw new ArgumentException("Essential parameters were not provided, Please enter the lookback hour or a DateRange");
            }
            else if (args.Length == 2)
            {
                fromDate = DateTime.Parse(args[0]).ToString("yyyy-MM-ddTHH:mm:ss");
                toDate = DateTime.Parse(args[1]).ToString("yyyy-MM-ddTHH:mm:ss");
                logmessages = sumoHelper.Fetchlogs(fromDate, toDate).Result;
            }
            else if(args.Length == 1)
            {
                fromDate = DateTime.Now.AddMinutes(-(int.Parse(args[0]))).ToString("yyyy-MM-ddTHH:mm:ss");
                toDate = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                logmessages = sumoHelper.Fetchlogs(fromDate, toDate).Result;
            }

            foreach (var logmessage in logmessages)
            {
                try
                {
                    dynamic rawlogdata = JsonConvert.DeserializeObject<dynamic>(logmessage.ToString());
                    string rawData = rawlogdata.map._raw;

                    string[] lines = rawData.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);

                    string[] parts = lines[2].Split(new string[] { "Message: " }, StringSplitOptions.RemoveEmptyEntries);

                    if (parts.Length == 2)
                    {
                        string headerPart = parts[0].Replace("\\\"logData\\\": \\\"", "");
                        string messagePart = parts[1].Replace("\\\\", "\\").Replace("\\r\\n", "").Replace("\\\"", "\"").TrimEnd(new char[] { '"', ',' });

                        //Deserialize the message
                        var message = JsonConvert.DeserializeObject<PackageInduct>(messagePart);

                        //Extract the fields from header
                        string[] headerLines = headerPart.Split('.');
                        var headers = new Dictionary<string, string>();
                        foreach (string line in headerLines)
                        {
                            string[] keyValue = line.Trim().Split(':');
                            if (keyValue.Length == 2)
                            {
                                string key = keyValue[0].Trim();
                                string value = keyValue[1].Trim();
                                headers.Add(key, value);
                            }
                        }

                        producer.Produce<PackageInduct>(message, headers, topic, SUBJECT_PACKAGEINDUCT);
                        Thread.Sleep(2000);
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogException("Exception occured", ex);
                    continue;
                }
            }
            Logger.LogSeperator();
            Logger.DeleteOldLogFiles();
        }
    }
}
