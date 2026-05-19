namespace SupplyOn_Blazor.Models;

// ============================================================
// Representa una fila de la tabla TRANSAC con CODCMP='RQ'.
// Algunos campos vienen de la pantalla de carga pero NO existen
// en TRANSAC (Budget, Solicitante, Rubro, Subrubro) — quedan
// marcados con TODO SQL para que el desarrollador los conecte
// a la tabla/columna real.
// ============================================================
public class Requerimiento
{
    // --- Campos directos de TRANSAC ---
    public int      NroTrans       { get; set; }              // NROTRANS
    public DateTime? Fecha          { get; set; }             // FECHA
    public DateTime? FechaRegistro  { get; set; }             // FECHAREG (fecha de creación)
    public DateTime? FechaRequerida { get; set; }             // FECHAEXT (puede ser NULL en BD)
    public string   Prefijo        { get; set; } = string.Empty; // PREFIJO o XXXPREFIJO
    public string   Usuario        { get; set; } = string.Empty; // USERNAME (quien lo cargó)
    public string   Observacion    { get; set; } = string.Empty; // OBSERVACION
    public string   CodDepto       { get; set; } = string.Empty; // CODDEPTO
    public string   EstadoAut      { get; set; } = string.Empty; // ESTADOAUT (A=Autorizado)
    public decimal  Total          { get; set; }              // TOTAL
    public string?  FlgEli         { get; set; }              // FLGELI (E=Eliminado)

    // --- Campos derivados / TODO SQL (no están directos en TRANSAC) ---

    // TODO SQL: Budget se guarda en una tabla nueva (ver doc funcional).
    //   SELECT budget_codigo FROM Requerimiento_Budget WHERE nrotrans=@NroTrans
    public string Budget { get; set; } = string.Empty;

    // TODO SQL: Solicitante (texto libre opcional, agregar columna o tabla aux).
    //   SELECT solicitante FROM Requerimiento_Extra WHERE nrotrans=@NroTrans
    public string Solicitante { get; set; } = string.Empty;

    // TODO SQL: Rubro / Subrubro (auto desde Budget en la tabla Rubro).
    public string Rubro    { get; set; } = string.Empty;
    public string Subrubro { get; set; } = string.Empty;

    // --- Calculados ---

    // El doc define: <=5 días Urgente, 6-10 Media, >10 Estándar.
    public string TipoPedido
    {
        get
        {
            if (FechaRequerida is null) return "—";
            int dias = (FechaRequerida.Value.Date - DateTime.Today).Days;
            if (dias <= 5)  return "Urgente";
            if (dias <= 10) return "Media";
            return "Estándar";
        }
    }

    // ESTADOAUT='A' = autorizado, no se puede editar ni eliminar.
    public bool EstaAutorizado => string.Equals(EstadoAut, "A", StringComparison.OrdinalIgnoreCase);

    public string EstadoLegible => EstaAutorizado ? "AUTORIZADO" : "PENDIENTE";
}
