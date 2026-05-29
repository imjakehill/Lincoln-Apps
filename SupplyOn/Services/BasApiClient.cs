using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using SupplyOn_Blazor.Models;

namespace SupplyOn_Blazor.Services;

/// <summary>
/// Resultado detallado de la llamada al WebApi BAS. Si IsSuccess es false,
/// Status/RawBody contienen la respuesta cruda del servidor para diagnóstico.
/// </summary>
public class BasApiResult
{
    public bool                 IsSuccess    { get; set; }
    public int                  Status       { get; set; }
    public string               Endpoint     { get; set; } = string.Empty;
    public string               RequestJson  { get; set; } = string.Empty;
    public string               RawBody      { get; set; } = string.Empty;
    public string?              ErrorMessage { get; set; }   // excepción de red, etc.
    public ComprobanteResponse? Response     { get; set; }
}

// ============================================================
// Cliente HTTP para el WebApi BAS.
// Basado en TODOBASWEBAPI/WebApi BAS Prueba Funcional/BASApiClient.cs
// El token se toma desde SesionService (lo dejó el login).
// La URL base se lee de appsettings.json -> "Bas:BaseUrl"
// ============================================================
public class BasApiClient
{
    private readonly IHttpClientFactory _httpFactory;
    private readonly SesionService _sesion;
    private readonly string _baseUrl;

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        // BAS usa PascalCase, así que NO cambiamos PropertyNamingPolicy.
        PropertyNamingPolicy = null,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = true
    };

    public BasApiClient(IHttpClientFactory httpFactory, SesionService sesion, IConfiguration config)
    {
        _httpFactory = httpFactory;
        _sesion = sesion;
        // TODO: agregar la sección "Bas:BaseUrl" en appsettings.json
        _baseUrl = (config["Bas:BaseUrl"] ?? "http://localhost:5001").TrimEnd('/');
    }

    public async Task<BasApiResult> EnviarComprobanteCompraAsync(ComprobanteCompraRequest comprobante)
    {
        string jsonBody = JsonSerializer.Serialize(comprobante, _jsonOptions);
        string endpoint = $"{_baseUrl}/api/ComprobantesCompra";

        var result = new BasApiResult
        {
            Endpoint    = endpoint,
            RequestJson = jsonBody,
        };

        Debug.WriteLine("=== JSON que se envía (COMPRA) ===");
        Debug.WriteLine(jsonBody);
        Debug.WriteLine("==================================");

        try
        {
            var client = _httpFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", _sesion.AccessToken);
            client.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/json"));

            var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");

            HttpResponseMessage response = await client.PostAsync(endpoint, content);
            string responseBody = await response.Content.ReadAsStringAsync();

            result.Status  = (int)response.StatusCode;
            result.RawBody = responseBody;

            Debug.WriteLine($"Status: {result.Status} {response.StatusCode}");
            Debug.WriteLine("=== Respuesta del servidor ===");
            Debug.WriteLine(responseBody);
            Debug.WriteLine("==============================");

            if (response.StatusCode == System.Net.HttpStatusCode.Created)
            {
                result.IsSuccess = true;
                try { result.Response = JsonSerializer.Deserialize<ComprobanteResponse>(responseBody, _jsonOptions); }
                catch { /* respuesta no parseable; igual fue 201 */ }
            }
        }
        catch (Exception ex)
        {
            result.ErrorMessage = ex.Message;
            Debug.WriteLine($"[BasApiClient] Excepción: {ex}");
        }

        return result;
    }
}
