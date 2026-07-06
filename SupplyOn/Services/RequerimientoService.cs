using System.Text;
using Microsoft.Data.SqlClient;
using SupplyOn_Blazor.Models;

namespace SupplyOn_Blazor.Services;

// ============================================================
// Servicio de consulta/actualización/eliminación lógica de RQs.
// Usa la tabla TRANSAC del SQL Server <Lincoln2026>.
// Connection string: appsettings.json -> ConnectionStrings:Lincoln
// ============================================================
public class RequerimientoService
{
    private readonly string _connectionString;

    public RequerimientoService(IConfiguration config)
    {
        _connectionString = config.GetConnectionString("Lincoln")
            ?? throw new InvalidOperationException("ConnectionStrings:Lincoln no está configurado");
    }

    // ────────────────────────────────────────────────────────────
    //   LISTADO PAGINADO (lazy: trae solo el página actual)
    //   Usa OFFSET/FETCH de SQL Server.
    //   Soporta filtros opcionales por todos los campos.
    // ────────────────────────────────────────────────────────────
    public Task<(List<Requerimiento> Items, int Total)> GetPaginaAsync(int pagina, int filasPorPagina)
        => GetPaginaAsync(new RequerimientoFiltro(), pagina, filasPorPagina);

    // ────────────────────────────────────────────────────────────
    //   ÚLTIMAS PENDIENTES (uso exclusivo de Home.razor)
    //   Filtra ESTADOAUT IS NULL OR <> 'A'.
    // ────────────────────────────────────────────────────────────
    public Task<(List<Requerimiento> Items, int Total)> GetUltimasPendientesAsync(int pagina, int filasPorPagina)
        => GetPaginaAsync(new RequerimientoFiltro { Estado = "P" }, pagina, filasPorPagina);

    public async Task<(List<Requerimiento> Items, int Total)> GetPaginaAsync(
        RequerimientoFiltro filtro, int pagina, int filasPorPagina)
    {
        if (pagina < 1) pagina = 1;
        int offset = (pagina - 1) * filasPorPagina;

        // Construyo el WHERE dinámico parametrizado.
        var where = new StringBuilder();
        where.Append("CODCMP='RQ' AND (FLGELI IS NULL OR FLGELI <> 'E')");

        var parametros = new List<SqlParameter>();

        if (filtro.NroTransDesde.HasValue)
        {
            where.Append(" AND NROTRANS >= @NroDesde");
            parametros.Add(new SqlParameter("@NroDesde", filtro.NroTransDesde.Value));
        }
        if (filtro.NroTransHasta.HasValue)
        {
            where.Append(" AND NROTRANS <= @NroHasta");
            parametros.Add(new SqlParameter("@NroHasta", filtro.NroTransHasta.Value));
        }
        if (!string.IsNullOrWhiteSpace(filtro.Prefijo))
        {
            where.Append(" AND (PREFIJO LIKE @Prefijo OR XXXPREFIJO LIKE @Prefijo)");
            parametros.Add(new SqlParameter("@Prefijo", $"%{filtro.Prefijo}%"));
        }
        if (!string.IsNullOrWhiteSpace(filtro.Usuario))
        {
            where.Append(" AND USERNAME LIKE @Usuario");
            parametros.Add(new SqlParameter("@Usuario", $"%{filtro.Usuario}%"));
        }
        if (!string.IsNullOrWhiteSpace(filtro.Departamento))
        {
            where.Append(" AND CODDEPTO LIKE @Depto");
            parametros.Add(new SqlParameter("@Depto", $"%{filtro.Departamento}%"));
        }
        if (!string.IsNullOrWhiteSpace(filtro.Observacion))
        {
            where.Append(" AND OBSERVACION LIKE @Obs");
            parametros.Add(new SqlParameter("@Obs", $"%{filtro.Observacion}%"));
        }
        if (filtro.FechaCreacionDesde.HasValue)
        {
            where.Append(" AND FECHAREG >= @FechaCreaDesde");
            parametros.Add(new SqlParameter("@FechaCreaDesde", filtro.FechaCreacionDesde.Value.Date));
        }
        if (filtro.FechaCreacionHasta.HasValue)
        {
            where.Append(" AND FECHAREG < DATEADD(day, 1, @FechaCreaHasta)");
            parametros.Add(new SqlParameter("@FechaCreaHasta", filtro.FechaCreacionHasta.Value.Date));
        }
        if (filtro.FechaRequeridaDesde.HasValue)
        {
            where.Append(" AND FECHAEXT >= @FechaReqDesde");
            parametros.Add(new SqlParameter("@FechaReqDesde", filtro.FechaRequeridaDesde.Value.Date));
        }
        if (filtro.FechaRequeridaHasta.HasValue)
        {
            where.Append(" AND FECHAEXT < DATEADD(day, 1, @FechaReqHasta)");
            parametros.Add(new SqlParameter("@FechaReqHasta", filtro.FechaRequeridaHasta.Value.Date));
        }
        if (!string.IsNullOrWhiteSpace(filtro.TipoPedido))
        {
            // Calculado en SQL: días = FECHAEXT - HOY
            switch (filtro.TipoPedido.ToLower())
            {
                case "urgente":
                    where.Append(" AND FECHAEXT IS NOT NULL AND DATEDIFF(day, CAST(GETDATE() AS DATE), FECHAEXT) <= 5");
                    break;
                case "media":
                    where.Append(" AND FECHAEXT IS NOT NULL AND DATEDIFF(day, CAST(GETDATE() AS DATE), FECHAEXT) BETWEEN 6 AND 10");
                    break;
                case "estandar":
                    where.Append(" AND FECHAEXT IS NOT NULL AND DATEDIFF(day, CAST(GETDATE() AS DATE), FECHAEXT) > 10");
                    break;
            }
        }
        if (!string.IsNullOrWhiteSpace(filtro.Estado))
        {
            if (filtro.Estado.Equals("A", StringComparison.OrdinalIgnoreCase))
                where.Append(" AND ESTADOAUT = 'A'");
            else if (filtro.Estado.Equals("P", StringComparison.OrdinalIgnoreCase))
                where.Append(" AND (ESTADOAUT IS NULL OR ESTADOAUT <> 'A')");
        }

        // TODO SQL: cuando existan las tablas Budget / Requerimiento_Extra, aplicar JOIN+filtro.
        //   if (!string.IsNullOrWhiteSpace(filtro.Budget))       WHERE b.budget_codigo LIKE @Budget
        //   if (!string.IsNullOrWhiteSpace(filtro.Solicitante))  WHERE re.solicitante LIKE @Solicitante

        string sqlCount = $@"SELECT COUNT(*) FROM TRANSAC WHERE {where}";

        string sqlPagina = $@"
            SELECT
                NROTRANS,
                FECHA,
                FECHAREG,
                FECHAEXT,
                ISNULL(PREFIJO, XXXPREFIJO) AS PREFIJO,
                USERNAME,
                OBSERVACION,
                CODDEPTO,
                ESTADOAUT,
                TOTAL,
                FLGELI
            FROM TRANSAC
            WHERE {where}
            ORDER BY NROTRANS DESC
            OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY";

        var items = new List<Requerimiento>();
        int total = 0;

        using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync();

        // COUNT
        using (var cmd = new SqlCommand(sqlCount, cn))
        {
            foreach (var p in parametros) cmd.Parameters.Add(CloneParam(p));
            object? raw = await cmd.ExecuteScalarAsync();
            total = raw is int i ? i : 0;
        }

        // Página
        using (var cmd = new SqlCommand(sqlPagina, cn))
        {
            foreach (var p in parametros) cmd.Parameters.Add(CloneParam(p));
            cmd.Parameters.AddWithValue("@Offset", offset);
            cmd.Parameters.AddWithValue("@PageSize", filasPorPagina);

            using var rd = await cmd.ExecuteReaderAsync();
            while (await rd.ReadAsync())
            {
                items.Add(new Requerimiento
                {
                    NroTrans       = rd.GetInt32(rd.GetOrdinal("NROTRANS")),
                    Fecha          = ObtenerFecha(rd, "FECHA"),
                    FechaRegistro  = ObtenerFecha(rd, "FECHAREG"),
                    FechaRequerida = ObtenerFecha(rd, "FECHAEXT"),
                    Prefijo        = ObtenerString(rd, "PREFIJO"),
                    Usuario        = ObtenerString(rd, "USERNAME"),
                    Observacion    = ObtenerString(rd, "OBSERVACION"),
                    CodDepto       = ObtenerString(rd, "CODDEPTO"),
                    EstadoAut      = ObtenerString(rd, "ESTADOAUT"),
                    Total          = ObtenerDecimal(rd, "TOTAL"),
                    FlgEli         = ObtenerStringNullable(rd, "FLGELI"),

                    // TODO SQL: completar con JOIN a tablas Budget / Rubro / Requerimiento_Extra
                    Budget      = string.Empty,
                    Solicitante = string.Empty,
                    Rubro       = string.Empty,
                    Subrubro    = string.Empty,
                });
            }
        }

        return (items, total);
    }

    // Los SqlParameter no se pueden reutilizar entre commands.
    private static SqlParameter CloneParam(SqlParameter src)
        => new SqlParameter(src.ParameterName, src.Value);

    // ────────────────────────────────────────────────────────────
    //   PRÓXIMO N° DE REQUERIMIENTO
    //   SELECT ISNULL(MAX(NROTRANS),0)+1 FROM TRANSAC WHERE CODCMP='RQ'
    //   Nota: BAS termina asignando el N° real al recibir el POST con
    //   Numero=0; esto es sólo informativo para la UI.
    // ────────────────────────────────────────────────────────────
    public async Task<int> GetProximoNumeroAsync()
    {
        const string sql = @"
            SELECT ISNULL(MAX(NROTRANS), 0) + 1
            FROM TRANSAC
            WHERE CODCMP = 'RQ'";

        using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync();
        using var cmd = new SqlCommand(sql, cn);
        object? raw = await cmd.ExecuteScalarAsync();
        return raw is int i ? i : Convert.ToInt32(raw ?? 1);
    }

    // ────────────────────────────────────────────────────────────
    //   PREFIJO DEL TALONARIO SEGÚN USUARIO LOGUEADO
    //   El prefijo del RQ sale del talonario habilitado para el usuario.
    //   Devuelve null si el usuario no tiene talonario RQ o XXXPREFIJO es NULL.
    // ────────────────────────────────────────────────────────────
    public async Task<(string? Prefijo, string? Talonario)> GetPrefijoUsuarioAsync(string usuario)
    {
        const string sql = @"
            SELECT TOP 1 a.TALONARIO, b.XXXPREFIJO
            FROM PERMISOSTAL a
            JOIN TALONARIOS b ON a.talonario = b.talonario
            WHERE a.CODCMP = 'RQ' AND a.usuario = @Usuario";

        using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync();
        using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.AddWithValue("@Usuario", (object?)usuario ?? DBNull.Value);
        using var rd = await cmd.ExecuteReaderAsync();

        if (!await rd.ReadAsync()) return (null, null);

        string? talonario = rd.IsDBNull(0) ? null : rd.GetValue(0)?.ToString()?.Trim();
        string? prefijo   = rd.IsDBNull(1) ? null : rd.GetValue(1)?.ToString()?.Trim();
        if (string.IsNullOrWhiteSpace(talonario)) talonario = null;
        if (string.IsNullOrWhiteSpace(prefijo))   prefijo   = null;
        return (prefijo, talonario);
    }

    // ────────────────────────────────────────────────────────────
    //   CENTROS DE APROPIACIÓN DEL USUARIO (= su departamento)
    //   Mapeo usuario→centro en USUARIO_CENTROAP; la descripción (NOMBRE)
    //   sale de centrosap con el join por CODCEN (el prefijo NO se usa acá).
    //   LEFT JOIN + fallback a CODCEN por si un centro no está en centrosap.
    // ────────────────────────────────────────────────────────────
    public async Task<List<(string Prefijo, string CodCen, string Nombre)>> GetCentrosApUsuarioAsync(string usuario)
    {
        const string sql = @"
            SELECT u.PREFIJO, u.CODCEN, c.NOMBRE
            FROM USUARIO_CENTROAP u
            LEFT JOIN centrosap c ON c.CODCEN = u.CODCEN
            WHERE u.USUARIO = @Usuario AND u.ACTIVO = 1
            ORDER BY c.NOMBRE";

        var result = new List<(string, string, string)>();

        using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync();
        using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.AddWithValue("@Usuario", (object?)usuario ?? DBNull.Value);
        using var rd = await cmd.ExecuteReaderAsync();
        while (await rd.ReadAsync())
        {
            string prefijo = ObtenerString(rd, "PREFIJO");
            string codcen  = ObtenerString(rd, "CODCEN");
            string nombre  = ObtenerString(rd, "NOMBRE");
            if (string.IsNullOrWhiteSpace(nombre)) nombre = codcen; // sin match en centrosap
            result.Add((prefijo, codcen, nombre));
        }
        return result;
    }

    // ────────────────────────────────────────────────────────────
    //   ELIMINACIÓN LÓGICA
    //   UPDATE TRANSAC SET FLGELI='E' WHERE NROTRANS=@nro
    //   Sólo si NO está autorizado (ESTADOAUT distinto de 'A').
    // ────────────────────────────────────────────────────────────
    public async Task<bool> EliminarAsync(int nroTrans)
    {
        const string sql = @"
            UPDATE TRANSAC
            SET FLGELI = 'E', ELIMINACION = GETDATE()
            WHERE NROTRANS = @Nro
              AND CODCMP = 'RQ'
              AND (ESTADOAUT IS NULL OR ESTADOAUT <> 'A')";

        using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync();
        using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.AddWithValue("@Nro", nroTrans);
        int filas = await cmd.ExecuteNonQueryAsync();
        return filas > 0;
    }

    // ────────────────────────────────────────────────────────────
    //   CATÁLOGO DE ITEMS (modal lupa de CreateReq)
    //   SELECT CODITEM, DESCRIPCION FROM ITEMS
    // ────────────────────────────────────────────────────────────
    public async Task<List<(string Codigo, string Descripcion, string Rubro, string Subrubro, string Unidad1, string Unidad2)>> GetCatalogoItemsAsync()
    {
        // Rubro (CODRUB), Subrubro (CODSBR) y las Unidades de Medida (unidad1 / unidad2)
        // viven en la misma tabla ITEMS, así que se traen junto al código/descripción en
        // esta única consulta (que ya se carga una sola vez al abrir la pantalla). Al
        // seleccionar un ítem por la lupa esos valores se autocompletan desde memoria.
        const string sql = @"
            SELECT CODITM, DESCRIPCION, CODRUB, CODSBR, unidad1, unidad2
            FROM ITEMS
            WHERE SUSPENDIDOS=0
            ORDER BY DESCRIPCION ASC";

        var result = new List<(string, string, string, string, string, string)>();

        using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync();
        using var cmd = new SqlCommand(sql, cn);
        using var rd = await cmd.ExecuteReaderAsync();
        while (await rd.ReadAsync())
        {
            // Rubro y Subrubro: si vienen NULL/vacío se muestran como "NA" (igual que FANTASIA).
            string rubro    = ObtenerString(rd, "CODRUB");
            string subrubro = ObtenerString(rd, "CODSBR");

            result.Add((
                ObtenerString(rd, "CODITM"),
                ObtenerString(rd, "DESCRIPCION"),
                string.IsNullOrWhiteSpace(rubro)    ? "NA" : rubro,
                string.IsNullOrWhiteSpace(subrubro) ? "NA" : subrubro,
                // Unidades crudas: la lógica NA / combobox se resuelve en la UI según
                // cuántas de las dos traigan valor.
                ObtenerString(rd, "unidad1"),
                ObtenerString(rd, "unidad2")
            ));
        }

        return result;
    }

    // ────────────────────────────────────────────────────────────
    //   CATÁLOGO DE PROVEEDORES (modal lupa de Proveedor Sug.)
    //   SELECT CODCTACTE, NOMBRE, FANTASIA FROM ctactes WHERE cueprefi='P'
    //   FANTASIA en NULL se reemplaza por "NA".
    // ────────────────────────────────────────────────────────────
    public async Task<List<(string Codigo, string Nombre, string Fantasia)>> GetCatalogoProveedoresAsync()
    {
        const string sql = @"
            SELECT CODCTACTE, NOMBRE, FANTASIA
            FROM ctactes
            WHERE cueprefi = 'P'
            ORDER BY NOMBRE ASC";

        var result = new List<(string, string, string)>();

        using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync();
        using var cmd = new SqlCommand(sql, cn);
        using var rd = await cmd.ExecuteReaderAsync();
        while (await rd.ReadAsync())
        {
            string fantasia = ObtenerStringNullable(rd, "FANTASIA") ?? "NA";
            if (string.IsNullOrWhiteSpace(fantasia)) fantasia = "NA";

            result.Add((
                ObtenerString(rd, "CODCTACTE"),
                ObtenerString(rd, "NOMBRE"),
                fantasia
            ));
        }
        return result;
    }

    // ────────────────────────────────────────────────────────────
    //   ACTUALIZACIÓN (desde el modal de edición)
    //   Sólo campos editables: FechaRequerida (FECHAEXT), Prefijo,
    //   Observación y Solicitante.
    //   Budget / Rubro / Subrubro / NroTrans NO se editan.
    //   Sólo si NO está autorizado.
    // ────────────────────────────────────────────────────────────
    public async Task<bool> ActualizarAsync(Requerimiento r)
    {
        const string sql = @"
            UPDATE TRANSAC
            SET FECHAEXT    = @FechaRequerida,
                PREFIJO     = @Prefijo,
                OBSERVACION = @Observacion
            WHERE NROTRANS  = @Nro
              AND CODCMP    = 'RQ'
              AND (ESTADOAUT IS NULL OR ESTADOAUT <> 'A')";

        using var cn = new SqlConnection(_connectionString);
        await cn.OpenAsync();
        using var cmd = new SqlCommand(sql, cn);
        cmd.Parameters.AddWithValue("@Nro", r.NroTrans);
        cmd.Parameters.AddWithValue("@FechaRequerida", (object?)r.FechaRequerida ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Prefijo", (object?)r.Prefijo ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Observacion", (object?)r.Observacion ?? DBNull.Value);
        int filas = await cmd.ExecuteNonQueryAsync();

        // TODO SQL: el campo Solicitante no está en TRANSAC. Cuando crees la
        // tabla auxiliar Requerimiento_Extra, hacer UPSERT aquí.
        // INSERT INTO Requerimiento_Extra (nrotrans, solicitante)
        //   VALUES (@Nro, @Solicitante)
        //   ON CONFLICT (nrotrans) DO UPDATE SET solicitante = @Solicitante;

        return filas > 0;
    }

    // ────────────────────────────────────────────────────────────
    //   Helpers de lectura segura del SqlDataReader
    // ────────────────────────────────────────────────────────────
    private static DateTime? ObtenerFecha(SqlDataReader rd, string col)
    {
        int i = rd.GetOrdinal(col);
        return rd.IsDBNull(i) ? null : rd.GetDateTime(i);
    }

    private static string ObtenerString(SqlDataReader rd, string col)
    {
        int i = rd.GetOrdinal(col);
        return rd.IsDBNull(i) ? string.Empty : rd.GetString(i).Trim();
    }

    private static string? ObtenerStringNullable(SqlDataReader rd, string col)
    {
        int i = rd.GetOrdinal(col);
        return rd.IsDBNull(i) ? null : rd.GetString(i).Trim();
    }

    private static decimal ObtenerDecimal(SqlDataReader rd, string col)
    {
        int i = rd.GetOrdinal(col);
        return rd.IsDBNull(i) ? 0m : rd.GetDecimal(i);
    }
}
