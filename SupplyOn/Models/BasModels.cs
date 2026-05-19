namespace SupplyOn_Blazor.Models;

// ============================================================
// Modelos para llamar al WebApi BAS (POST /api/ComprobantesCompra)
// Basados en TODOBASWEBAPI/WebApi BAS Prueba Funcional/Model.cs
// BAS usa PascalCase en el JSON, no tocar el naming.
// ============================================================

public class ComprobanteCompraRequest
{
    public string FechaCreacion { get; set; } = string.Empty;
    public string Fecha { get; set; } = string.Empty;
    public string Comprobante { get; set; } = string.Empty; // RQ o OC
    public string Prefijo { get; set; } = string.Empty;
    public int Numero { get; set; }                  // 0 = autoincremental
    public string Proveedor { get; set; } = string.Empty;
    public string Nombre { get; set; } = string.Empty;
    public string MonedaCtaCte { get; set; } = string.Empty;
    public string MonedaComprobante { get; set; } = string.Empty;
    public decimal TotalGravado { get; set; }
    public decimal TotalIva { get; set; }
    public decimal Total { get; set; }
    public string MetodoPago { get; set; } = string.Empty;   // C = contado
    public string Caja { get; set; } = string.Empty;
    public string Comprador { get; set; } = string.Empty;
    public int Empresa { get; set; }
    public int ImputacionContable { get; set; }              // max 9 dígitos
    public string ObservacionComprobante { get; set; } = string.Empty;
    public int Sucursal { get; set; }
    public int Deposito { get; set; }
    public string Usuario { get; set; } = string.Empty;
    public string NumeroCAIoCAE { get; set; } = string.Empty;
    public string VencimientoCAIoCAE { get; set; } = string.Empty;
    public List<ItemRequest> Items { get; set; } = new();
    public List<Vencimiento> Vencimientos { get; set; } = new();
    public List<Efectivo> Efectivos { get; set; } = new();
}

public class ItemRequest
{
    public string CodigoItem { get; set; } = string.Empty;
    public string NumeroUnidadMedida { get; set; } = string.Empty;
    public decimal CantidadPrimeraUnidad { get; set; }
    public decimal PrecioUnitario { get; set; }
    public decimal ImporteTotal { get; set; }
    public decimal ImporteGravado { get; set; }
    public decimal ImporteIva { get; set; }
    public decimal TasaIva { get; set; }
    public string PendienteRemitirFacturar { get; set; } = string.Empty; // A = a recibir
    public string TipoEntrega { get; set; } = string.Empty;              // O = orden de compra
}

public class Vencimiento
{
    public string FechaVencimiento { get; set; } = string.Empty;
    public decimal Importe { get; set; }
}

public class Efectivo
{
    public string MedioPago { get; set; } = string.Empty;
    public decimal Importe { get; set; }
    public int Cantidad { get; set; }
}

// --- Respuesta del servidor ---

public class ComprobanteResponse
{
    public int IdTransaccion { get; set; }
    public bool Eliminable { get; set; }
    public string? Motivo { get; set; }
    public List<ComprobanteCreado>? Comprobantes { get; set; }
}

public class ComprobanteCreado
{
    public string? Fecha { get; set; }
    public string? Comprobante { get; set; }
    public string? Prefijo { get; set; }
    public string? Numero { get; set; }
    public string? NumeroFinal { get; set; }
    public string? FechaComprobanteExterno { get; set; }
    public string? PrefijoComprobanteExterno { get; set; }
    public string? NumeroComprobanteExterno { get; set; }
}
