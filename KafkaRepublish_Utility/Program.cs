using KafkaRepublish_Utility.Helper;
using KafkaRepublish_Utility.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading;

namespace KafkaRepublish_Utility
{
    public class Program
    {
        private static string topic = System.Configuration.ConfigurationManager.AppSettings.Get("Kafka_Topic");
        private const string SUBJECT_PACKAGEINDUCT = "PACKAGE.INITIALINDUCT";
        static void Main(string[] args)
        {
            IEnumerable<string> errorlogs = null;
            
            /* 
             * Arguments that can be supplied:
             * 'FromDate' of format yyyy/MM/dd
             * 'ToDate' of format yyyy/MM/dd
             * 'Rerun' flag to identify is we want to run the intellicore failures or this utility failures
             * 'Hours' to give the user an option to run it based on how many hours he wants the application to lookback if he decides not to run with date range
            */
            
            if (args.Length < 3)
            {
                throw new ArgumentException("Essential parameters were not provided, Please enter the lookback hour or a DateRange and wether its a rerun of utility failure" +
                    "with the facilityname like 'IND3'");
            }
            else
            {
                bool rerun;
                if (args.Length == 4)
                {
                    if (DateTime.TryParseExact(args[0], "yyyy/MM/dd", null, System.Globalization.DateTimeStyles.None, out DateTime parsedFromDate) &&
                    DateTime.TryParseExact(args[1], "yyyy/MM/dd", null, System.Globalization.DateTimeStyles.None, out DateTime parsedToDate) &&
                    bool.TryParse(args[2], out bool parsedRerun))
                    {
                        string fromDate = parsedFromDate.ToString("yyyy-MM-dd HH:mm:ss.fff");
                        string toDate = parsedToDate.ToString("yyyy-MM-dd HH:mm:ss.fff");
                        rerun = parsedRerun;
                        string facility = args[3].ToUpper();
                        
                        UpdateConfigForFacility(facility);
                        DatabaseHelper dbhelper = new DatabaseHelper();
                        errorlogs = dbhelper.GetfailedMessages(fromDate, toDate, rerun);
                        
                    }
                }
                else if (args.Length == 3)
                {
                    if (int.TryParse(args[0], out int parsedMinute) && bool.TryParse(args[1], out bool parsedRerun))
                    {
                        int minutes = parsedMinute;
                        rerun = parsedRerun;
                        string facility = args[2].ToUpper();
                        
                        UpdateConfigForFacility(facility);
                        DatabaseHelper dbhelper = new DatabaseHelper();
                        errorlogs = dbhelper.GetFailedMessages(minutes, rerun);
                    }
                }
                else
                {
                    throw new ArgumentException("Too many arguments supplied. Either supply date range or hour, the rerun flag and the faciltyName like 'IND3'");
                }
                KafkaProducer producer = new KafkaProducer();
                producer.Setup();
                TryPublishingTheMessages(errorlogs, producer);
            }
        }

        private static void TryPublishingTheMessages(IEnumerable<string> errorlogs, KafkaProducer producer)
        {
            if (errorlogs.Any())
            {
                Console.WriteLine($"Found {errorlogs.Count()} error messages in the logs.");
                try
                {
                    foreach (string errorlog in errorlogs)
                    {
                        string[] parts = errorlog.Split(new string[] { "Message: " }, StringSplitOptions.RemoveEmptyEntries);

                        if (parts.Length == 2)
                        {
                            string headerPart = parts[0];
                            string messagePart = parts[1];

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
                        else
                        {
                            Console.WriteLine("Invalid message");
                        }
                    }
                    Console.WriteLine("Finished Publishing the messages");
                    producer.Flush();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error occured in message publishing: {e}");
                }
            }
            else
            {
                Console.WriteLine($"Found {errorlogs.Count()} error messages in the logs.");
            }
        }

        private static FacilityConfig facilityconfig;
        private static void UpdateConfigForFacility(string  facility)
        {
            facilityconfig = JsonConvert.DeserializeObject<FacilityConfig>(File.ReadAllText(@"InductionFacility.json"));
            Dictionary<string, string> facilityConfigDictionary = facilityconfig.InductionFacility.ToDictionary(facility => facility.FacilityName, facility => facility.DataSource);
            var datasource = facilityConfigDictionary[facility];

            var username = ConfigurationManager.AppSettings["username"];
            var password = ConfigurationManager.AppSettings["password"];
            string newConnectionString = $"Data Source={datasource};MultipleActiveResultSets=True;Initial Catalog=NgsCore;User ID={username};Password={password};Max Pool Size=1000";
            
            string configFilePath = AppDomain.CurrentDomain.SetupInformation.ConfigurationFile;
            ExeConfigurationFileMap configFileMap = new ExeConfigurationFileMap
            {
                ExeConfigFilename = configFilePath
            };

            Configuration config = ConfigurationManager.OpenMappedExeConfiguration(configFileMap, ConfigurationUserLevel.None);
            ConnectionStringSettings settings = config.ConnectionStrings.ConnectionStrings["NgsCoreConnectionString"];
            settings.ConnectionString = newConnectionString;
            config.Save(ConfigurationSaveMode.Modified);
            ConfigurationManager.RefreshSection("connectionStrings");
        }
    }
}
