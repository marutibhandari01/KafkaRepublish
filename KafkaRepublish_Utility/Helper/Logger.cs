using System;
using System.Configuration;
using System.IO;

namespace KafkaRepublish_Utility.Helper
{
    public static class Logger
    {
        private static string logFilePath;
        private static string logFolderPath;
        private static int logTTLInDays = Convert.ToInt32(ConfigurationManager.AppSettings.Get("LogTTL"));

        static Logger()
        {
            //Get the currrent directory
            string directory = Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory);

            //Path to the log folder
            logFolderPath = Path.Combine(directory, "log");

            logFilePath = Path.Combine(logFolderPath, $"log_{DateTime.Now:yyyy-MM-dd}.log");

            //Check if the log folder exists, if not, create it
            if(!Directory.Exists(logFolderPath))
            {
                Directory.CreateDirectory(logFolderPath);
            }

            //Check if the log file for today already exists, if not, create it
            if(!File.Exists(logFilePath))
            {
                File.Create(logFilePath).Close();
            }
        }

        public static void LogMessage(string errorType, string description, string data )
        {
            string formattedLogMessage = $"{DateTime.Now} - {errorType} - {description} - {data}";
            WriteToFile(formattedLogMessage);
        }

        public static void LogMessage(string message)
        {
            string formattedLogMessage = $"{DateTime.Now} - {message}";
            WriteToFile(formattedLogMessage);
        }

        public static void LogException(string message, Exception exception)
        {
            string formattedExceptionMessage = $"{DateTime.Now} - EXCEPTION - {message} - {exception}";
            WriteToFile(formattedExceptionMessage);
        }

        public static void LogSeperator()
        {
            WriteToFile("***********************************************************************************");
        }

        private static void WriteToFile(string message)
        {
            try
            {
                using (StreamWriter sw = new StreamWriter(logFilePath, true))
                {
                    sw.WriteLine(message);
                }
            }
            catch(Exception ex)
            {
                Logger.LogMessage($"Error writing to log File: {ex.Message}");
            }
        }

        public static void DeleteOldLogFiles()
        {
            string[] logFiles = Directory.GetFiles(logFolderPath, "*.log");

            foreach (var logFile in logFiles)
            {
                FileInfo fileInfo = new FileInfo(logFile);
                if (fileInfo.LastWriteTime < DateTime.Now.AddDays(-logTTLInDays))
                {
                    File.Delete(logFile);
                }
            }
        }
    }
}
