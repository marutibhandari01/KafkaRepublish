using Dapper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;


namespace KafkaRepublish_Utility.Helper
{
    public class DatabaseHelper
    {
        readonly string connectionString = null;
        readonly int commandTimeout = 0;
        public DatabaseHelper() 
        {
            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["NGSCore_ConnectionStringName"]].ConnectionString;
            commandTimeout = Convert.ToInt32(ConfigurationManager.AppSettings.Get("DbTimeout"));
        }

        public IEnumerable<string> GetfailedMessages(string fromDate, string toDate, Boolean rerun)
        {
            string sql1 = null;
            var parameter = new DynamicParameters();
            parameter.Add("@fromDate", fromDate, System.Data.DbType.AnsiString);
            parameter.Add("@toDate", toDate, System.Data.DbType.AnsiString);

            if (!rerun)
            {
                sql1 = @"select logdata from dbo.systemlog (nolock) 
                        where
                        1=1
                        and logdate BETWEEN @fromDate AND @toDate
                        and source = 'Newgistics.FastTrakIntegration.Kafka'
                        and Category = 'KafkaProducer'
                        and ComponentFunction = 'ProduceDeliveryReportHandler'
                        and logdesc like 'Error publishing message to Kafka topic%'";
            }
            else
            {
                sql1 = @"select logdata from dbo.systemlog (nolock) 
                        where
                        1=1
                        and logdate BETWEEN @fromDate AND @toDate
                        and source = 'KafkaRepublish_Utility.Kafka'
                        and Category = 'KafkaProducer'
                        and ComponentFunction = 'ProduceDeliveryReportHandler'
                        and logdesc like 'Error publishing message from KafkaRepublish_Utility to Kafka topic%'";
            }

            using (var db = new SqlConnection(connectionString))
            {
                try
                {
                    return db.Query<string>(sql1, parameter, commandTimeout: commandTimeout);
                }
                catch(SqlException ex)
                {
                    Console.WriteLine(ex.ToString());
                    throw;
                }
            }
        }

        public IEnumerable<string> GetFailedMessages(int minutes, bool rerun)
        {
            string sql1 = null;
            var parameter = new DynamicParameters();
            parameter.Add("@min", minutes, System.Data.DbType.Int32);
            
            if (!rerun)
            {
                sql1 = @"select logdata from dbo.systemlog (nolock) 
                        where
                        1=1
                        and logdate > dateadd(MINUTE, -@min, getutcdate())
                        and source = 'Newgistics.FastTrakIntegration.Kafka'
                        and Category = 'KafkaProducer'
                        and ComponentFunction = 'ProduceDeliveryReportHandler'
                        and logdesc like 'Error publishing message to Kafka topic%'";
            }
            else
            {
                sql1 = @"select logdata from dbo.systemlog (nolock) 
                        where
                        1=1
                        and logdate > dateadd(MINUTE, -@min, getutcdate())
                        and source = 'KafkaRepublish_Utility.Kafka'
                        and Category = 'KafkaProducer'
                        and ComponentFunction = 'ProduceDeliveryReportHandler'
                        and logdesc like 'Error publishing message from KafkaRepublish_Utility to Kafka topic%'";
            }

            using (var db = new SqlConnection(connectionString))
            {
                try
                {
                    return db.Query<string>(sql1, parameter, commandTimeout: commandTimeout);
                }
                catch (SqlException ex)
                {
                    Console.WriteLine(ex.ToString());
                    throw;
                }
            }
        }
    }
}
