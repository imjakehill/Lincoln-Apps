using System.Diagnostics;

namespace SupplyOn_Blazor.Helpers
{
    public class AppHelper
    {
        // Carpeta primaria: home del usuario que corre el proceso.
        // Carpeta fallback (server): C:\SupplyOn\logs (debe tener permisos para el AppPool).
        private static readonly string PrimaryLogDir =
            Path.Combine(@$"C:\Users\{Environment.UserName}", "logsSupplyOn");

        private static readonly string FallbackLogDir = @"C:\SupplyOn\logs";

        public void GenerateLog(string messageType, string message)
        {
            string fileName = $"log_{DateTime.Now:yyyy_MM_dd}.txt";
            string prefix   = messageType.ToLower() switch
            {
                "error"   => "[ERROR]",
                "warning" => "[WARNING]",
                "info"    => "[INFO]",
                _         => string.Empty,
            };

            string line = string.IsNullOrEmpty(prefix)
                ? $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}"
                : $"{prefix} {DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}";

            // Intento primario; si falla por permisos / path inexistente, voy al fallback.
            if (!TryWrite(PrimaryLogDir, fileName, line))
            {
                TryWrite(FallbackLogDir, fileName, line);
            }
        }

        private static bool TryWrite(string dir, string fileName, string line)
        {
            try
            {
                Directory.CreateDirectory(dir);
                string logFilePath = Path.Combine(dir, fileName);
                using var sw = new StreamWriter(logFilePath, append: true);
                sw.WriteLine(line);
                return true;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[AppHelper] No pude escribir log en '{dir}': {ex.Message}");
                return false;
            }
        }
    }
}
