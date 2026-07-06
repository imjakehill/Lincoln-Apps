namespace SupplyOn_Blazor.Models;

// ============================================================
// Modelos para llamar al WebApi BAS (POST /api/ComprobantesCompra)
// Basados en TODOBASWEBAPI/WebApi BAS Prueba Funcional/Model.cs
// BAS usa PascalCase en el JSON, no tocar el naming.
// ============================================================

public class ComprobanteCompraRequest
{
    // ── Obligatorios siempre ──────────────────────────────
    public string FechaCreacion  { get; set; } = string.Empty;
    public string Fecha          { get; set; } = string.Empty;
    //public string Proveedor      { get; set; } = string.Empty;
    //public string Nombre         { get; set; } = string.Empty; // Nombre del proveedor
    public string Comprobante    { get; set; } = string.Empty; // RQ o OC
    public string Prefijo        { get; set; } = string.Empty;
    public int    Numero         { get; set; }                 // 0 = autoincremental BAS
    public string Usuario        { get; set; } = string.Empty;

    // ── Ints estructurales (RQ requiere al menos Sucursal/Deposito/ImputacionContable) ──
    public int Empresa            { get; set; }
    public int Sucursal           { get; set; }
    public int ImputacionContable { get; set; }                // max 9 dígitos
    public int Deposito           { get; set; }

    // ── Opcionales: si quedan en null NO se serializan ────
    // (BasApiClient usa JsonIgnoreCondition.WhenWritingNull)
    public string?  Proveedor              { get; set; }       // requerido: da el CODCTACTE que BAS inserta en PROVEITEMS
    //public string?  Nombre                 { get; set; }       // OPCIONAL
    public string?  ObservacionComprobante { get; set; }       // OPCIONAL — se manda si tiene texto
    public string?  Comprador              { get; set; }

    // ── NO aplican a RQ — quedan nullable y se mandan null ──
    public string?  MonedaCtaCte       { get; set; }
    public string?  MonedaComprobante  { get; set; }
    public decimal? TotalGravado       { get; set; }
    public decimal? TotalIva           { get; set; }
    public decimal? Total              { get; set; }
    public string?  MetodoPago         { get; set; }           // C = contado (pago)
    public string?  Caja               { get; set; }           // (pago)
    public string?  NumeroCAIoCAE      { get; set; }           // (fiscal)
    public string?  VencimientoCAIoCAE { get; set; }           // (fiscal)

    public List<ItemRequest>  Items        { get; set; } = new();
    public List<Vencimiento>? Vencimientos { get; set; }       // null = no se serializa
    public List<Efectivo>?    Efectivos    { get; set; }       // null = no se serializa
}

public class ItemRequest
{
    // ── Obligatorios para RQ ─────────────────────────────
    public string PendienteRemitirFacturar { get; set; } = string.Empty; // A = a recibir
    public string TipoEntrega              { get; set; } = string.Empty; // O = orden de compra

    // ── Opcionales (omitir si null) ──────────────────────
    public string? CodigoItem         { get; set; }
    public string? NumeroUnidadMedida { get; set; }

    // Centro de Apropiación (CODCEN del centro elegido en la cabecera). BAS lo usa para
    // MVSITEMS.(CENPREFI='A' literal, CODCEN). Debe existir en CENTROSAP. "B" no se usa.
    // Nullable → si va null se omite y BAS usa el centro por defecto de la sucursal.
    public string? CentroApropiacionA { get; set; }

    public decimal? CantidadPrimeraUnidad { get; set; }

    //public decimal? CantidadSegundaUnidad { get; set; }
    public decimal? PrecioUnitario        { get; set; }
    public decimal? ImporteTotal          { get; set; }
    public decimal? ImporteGravado        { get; set; }
    public decimal? ImporteIva            { get; set; }
    public decimal? TasaIva               { get; set; }
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
