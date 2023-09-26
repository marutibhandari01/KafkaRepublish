using Confluent.Kafka;
using Newgistics.Common;
using Newgistics.Utility;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using Formatting = Newtonsoft.Json.Formatting;

namespace KafkaRepublish_Utility.Kafka
{
    public class KafkaProducer
    {
        private IProducer<string, string> Producer;
        private Action<DeliveryReport<string, string>> ProducerDeliverReportCallBack = new Action<DeliveryReport<string, string>>(ProduceDeliveryReportHandler);
        public KafkaProducer() { }

        public void Setup()
        {
            var clientId = ConfigurationManager.AppSettings.Get("Kafka_ClientId");
            var bootstrapServers = ConfigurationManager.AppSettings.Get("Kafka_bootstrapServers");
            var produceTimeOut = System.Convert.ToInt32(ConfigurationManager.AppSettings.Get("Kafka_MessageTimeoutMs"));
            int maxRetryAttempts = Convert.ToInt32(ConfigurationManager.AppSettings.Get("Kafka_maxRetryAttempts"));
            var waitTime = Convert.ToInt32(ConfigurationManager.AppSettings.Get("Kafka_waitTime"));
            //bool idempotence = Convert.ToBoolean(ConfigurationManager.AppSettings.Get("Kafka_EnableIdempotence"));
            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = clientId,
                MessageTimeoutMs = produceTimeOut,
                MessageSendMaxRetries = maxRetryAttempts,
                RetryBackoffMs = waitTime
                //EnableIdempotence = idempotence,
                //Acks = Acks.All
            };
            Producer = new ProducerBuilder<string, string>(config)
                .Build();
        }

        public void Flush()
        {
            Producer.Flush(TimeSpan.FromSeconds(2000));
        }

        private static string GetHeaderValue(IHeader header)
        {
            try
            {
                byte[] bytes = header.GetValueBytes();
                var value = System.Text.Encoding.ASCII.GetString(bytes);
                return value;
            }
            catch (Exception)
            {
                return string.Empty;
            }
        }

        private static void ProduceDeliveryReportHandler(DeliveryReport<string, string> deliveryreport)
        {
            try
            {
                StringBuilder sb = new StringBuilder(100);
                foreach (var item in deliveryreport.Headers)
                {
                    sb.Append(" ");
                    sb.Append(item.Key.ToUpper());
                    sb.Append(": ");
                    sb.Append(GetHeaderValue(item));
                    sb.Append(".");
                }

                if (deliveryreport.Error.Code != ErrorCode.NoError)
                {
                    NGSUtilityWrapper.LogMessage(Logger.LogEventType.Error,
                        NGSERROR.ERR_GENERAL,
                        $"Error publishing message from KafkaRepublish_Utility to Kafka topic '{deliveryreport.Topic}'. Error: {deliveryreport.Error.Reason}",
                        $"Key: {deliveryreport.Message.Key}. {sb.ToString()} Message: {deliveryreport.Message.Value}");
                }
                else
                {
                    NGSUtilityWrapper.LogMessage(Logger.LogEventType.Information,
                        NGSERROR.ERR_SUCCESS,
                        $"Produced message from KafkaRepublish_Utility to: {deliveryreport.TopicPartitionOffset}",
                        $"Key: {deliveryreport.Message.Key}. {sb.ToString()} Message: {deliveryreport.Message.Value}");
                }
            }
            catch (Exception ex)
            {
                NGSUtilityWrapper.LogException(ex.Message, NGSERROR.ERR_GENERAL, ex);
            }
        }

        public void Produce<TBody>(TBody body, Dictionary<string, string> headerValues, string topic, string subject)
        {
            string bodyString = JsonConvert.SerializeObject(body, new JsonSerializerSettings
            {
                DateFormatString = "yyyy-MM-ddTHH:mm:ss.fffZ",
                Formatting = Formatting.Indented
            });

            var headers = new Headers();
            headers.Add("content-type", System.Text.Encoding.ASCII.GetBytes(headerValues["CONTENT-TYPE"]));
            headers.Add("subject", System.Text.Encoding.ASCII.GetBytes(subject));
            headers.Add("version", System.Text.Encoding.ASCII.GetBytes(headerValues["VERSION"]));
            headers.Add("eventid", System.Text.Encoding.ASCII.GetBytes(headerValues["EVENTID"]));

            var key = headerValues["Key"];

            try
            {
                Producer.Produce(topic, new Message<string, string> { Key = key, Value = bodyString, Headers = headers }, ProducerDeliverReportCallBack);
            }
            catch (Exception ex)
            {
                NGSUtilityWrapper.LogException(
                                $"Exception occurred publishing message to Kafka topic '{topic}' from KafkaRepublish_Utility. Error: {ex.Message}" +
                                $"Key: {key}. " +
                                $"Message: {bodyString}",
                                NGSERROR.ERR_GENERAL,
                                ex);
            }
        }
    }
}
