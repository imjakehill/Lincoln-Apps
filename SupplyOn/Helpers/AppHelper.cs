using System.Diagnostics;
using System.Linq.Expressions;

namespace SupplyOn_Blazor.Helpers
{
    public class AppHelper
    {
        public async void GenerateLog(string messageType, string message)
        {
            string fileName = $"log_{DateTime.Now:yyyy_MM_dd}.txt";
            string currentUser = Environment.UserName;
            string logFilePath = Path.Combine(@$"C:\Users\{currentUser}\logsSupplyOn", fileName);

            try
            {
                using (StreamWriter sw = new StreamWriter(logFilePath, true)) // true es para indicar el append
                {
                    switch (messageType.ToLower())
                    {
                        case "error":
                            sw.WriteLine($"[ERROR] {DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}");
                            break;
                        case "warning":
                            sw.WriteLine($"[WARNING] {DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}");
                            break;
                        case "info":
                            sw.WriteLine($"[INFO] {DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}");
                            break;
                        default:
                            sw.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}");
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error al escribir en el log: {ex.Message}");
            }

            
        }
    }
}
