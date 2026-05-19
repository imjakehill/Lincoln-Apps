using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using SupplyOn_Blazor.Models;

namespace SupplyOn_Blazor.Services;

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

    public async Task<ComprobanteResponse?> EnviarComprobanteCompraAsync(ComprobanteCompraRequest comprobante)
    {
        string jsonBody = JsonSerializer.Serialize(comprobante, _jsonOptions);

        Debug.WriteLine("=== JSON que se envía (COMPRA) ===");
        Debug.WriteLine(jsonBody);
        Debug.WriteLine("==================================");

        var client = _httpFactory.CreateClient();
        client.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", _sesion.AccessToken);
        client.DefaultRequestHeaders.Accept.Add(
            new MediaTypeWithQualityHeaderValue("application/json"));

        var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
        string endpoint = $"{_baseUrl}/api/ComprobantesCompra";

        HttpResponseMessage response = await client.PostAsync(endpoint, content);
        string responseBody = await response.Content.ReadAsStringAsync();

        Debug.WriteLine($"Status: {(int)response.StatusCode} {response.StatusCode}");
        Debug.WriteLine("=== Respuesta del servidor ===");
        Debug.WriteLine(responseBody);
        Debug.WriteLine("==============================");

        if (response.StatusCode == System.Net.HttpStatusCode.Created)
            return JsonSerializer.Deserialize<ComprobanteResponse>(responseBody, _jsonOptions);

        Debug.WriteLine($"ERROR: {response.StatusCode} - {responseBody}");
        return null;
    }
}
