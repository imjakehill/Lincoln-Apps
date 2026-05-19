namespace SupplyOn_Blazor.Models;

// ============================================================
// Conjunto de filtros que el usuario puede aplicar en ViewReq.
// Todos los campos son opcionales (null = no filtra).
// La construcción del SQL dinámico está en RequerimientoService.
// ============================================================
public class RequerimientoFiltro
{
    // Identificación
    public int?    NroTransDesde { get; set; }
    public int?    NroTransHasta { get; set; }
    public string? Prefijo       { get; set; }

    // Texto libre por campo
    public string? Usuario       { get; set; }   // USERNAME LIKE
    public string? Departamento  { get; set; }   // CODDEPTO LIKE
    public string? Observacion   { get; set; }   // OBSERVACION LIKE
    public string? Budget        { get; set; }   // TODO SQL — no está en TRANSAC
    public string? Solicitante   { get; set; }   // TODO SQL — no está en TRANSAC

    // Fechas
    public DateTime? FechaCreacionDesde  { get; set; }   // FECHAREG >=
    public DateTime? FechaCreacionHasta  { get; set; }   // FECHAREG <=
    public DateTime? FechaRequeridaDesde { get; set; }   // FECHAEXT >=
    public DateTime? FechaRequeridaHasta { get; set; }   // FECHAEXT <=

    // Selects
    // null = todos | "Urgente" | "Media" | "Estandar"
    public string? TipoPedido { get; set; }

    // null = todos | "A" (autorizado) | "P" (pendiente, = ESTADOAUT IS NULL OR <>'A')
    public string? Estado { get; set; }

    public bool TieneAlgunFiltro =>
        NroTransDesde.HasValue
        || NroTransHasta.HasValue
        || !string.IsNullOrWhiteSpace(Prefijo)
        || !string.IsNullOrWhiteSpace(Usuario)
        || !string.IsNullOrWhiteSpace(Departamento)
        || !string.IsNullOrWhiteSpace(Observacion)
        || !string.IsNullOrWhiteSpace(Budget)
        || !string.IsNullOrWhiteSpace(Solicitante)
        || FechaCreacionDesde.HasValue
        || FechaCreacionHasta.HasValue
        || FechaRequeridaDesde.HasValue
        || FechaRequeridaHasta.HasValue
        || !string.IsNullOrWhiteSpace(TipoPedido)
        || !string.IsNullOrWhiteSpace(Estado);

    public int CantidadActivos
    {
        get
        {
            int c = 0;
            if (NroTransDesde.HasValue) c++;
            if (NroTransHasta.HasValue) c++;
            if (!string.IsNullOrWhiteSpace(Prefijo))      c++;
            if (!string.IsNullOrWhiteSpace(Usuario))      c++;
            if (!string.IsNullOrWhiteSpace(Departamento)) c++;
            if (!string.IsNullOrWhiteSpace(Observacion))  c++;
            if (!string.IsNullOrWhiteSpace(Budget))       c++;
            if (!string.IsNullOrWhiteSpace(Solicitante))  c++;
            if (FechaCreacionDesde.HasValue)  c++;
            if (FechaCreacionHasta.HasValue)  c++;
            if (FechaRequeridaDesde.HasValue) c++;
            if (FechaRequeridaHasta.HasValue) c++;
            if (!string.IsNullOrWhiteSpace(TipoPedido)) c++;
            if (!string.IsNullOrWhiteSpace(Estado))     c++;
            return c;
        }
    }

    public void Limpiar()
    {
        NroTransDesde = NroTransHasta = null;
        Prefijo = Usuario = Departamento = Observacion = Budget = Solicitante = null;
        FechaCreacionDesde = FechaCreacionHasta = null;
        FechaRequeridaDesde = FechaRequeridaHasta = null;
        TipoPedido = null;
        Estado = null;
    }
}
