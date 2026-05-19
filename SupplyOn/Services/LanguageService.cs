namespace SupplyOn_Blazor.Services;

// ============================================================
// Servicio de traducción simple (i18n).
// Registrado como Scoped: una instancia por circuito Blazor.
// Las páginas que quieran reaccionar al cambio deben suscribirse
// al evento OnChange en OnInitialized y desuscribirse en Dispose.
//
// Para extender a más pantallas, basta con agregar claves a los
// diccionarios _es y _en abajo.
// ============================================================
public class LanguageService
{
    public event Action? OnChange;

    public string Idioma { get; private set; } = "es";

    public void SetIdioma(string idioma)
    {
        if (string.IsNullOrWhiteSpace(idioma) || Idioma == idioma) return;
        if (idioma != "es" && idioma != "en") return;
        Idioma = idioma;
        OnChange?.Invoke();
    }

    /// <summary>Traduce una clave al idioma actual. Si no existe, devuelve la clave.</summary>
    public string T(string key)
    {
        var dict = Idioma == "en" ? _en : _es;
        return dict.TryGetValue(key, out var value) ? value : key;
    }

    // ────────────────────────────────────────────────────────────
    //   Diccionarios de traducciones
    // ────────────────────────────────────────────────────────────
    private static readonly Dictionary<string, string> _es = new()
    {
        // LOGIN
        ["login.page_title"]        = "SupplyOn — Inicio de Sesión",
        ["login.heading"]           = "Inicio de Sesión",
        ["login.username"]          = "Usuario",
        ["login.password"]          = "Contraseña",
        ["login.remember"]          = "Recordarme",
        ["login.toggle_password"]   = "Mostrar / ocultar contraseña",
        ["login.button"]            = "Iniciar Sesión",
        ["login.loading"]           = "Cargando...",
        ["login.error_empty"]       = "Completá usuario y contraseña.",
        ["login.error_credentials"] = "Credenciales incorrectas.",
        ["login.error_connection"]  = "Error de conexión",
    };

    private static readonly Dictionary<string, string> _en = new()
    {
        // LOGIN
        ["login.page_title"]        = "SupplyOn — Sign In",
        ["login.heading"]           = "Sign In",
        ["login.username"]          = "Username",
        ["login.password"]          = "Password",
        ["login.remember"]          = "Remember me",
        ["login.toggle_password"]   = "Show / hide password",
        ["login.button"]            = "Sign In",
        ["login.loading"]           = "Loading...",
        ["login.error_empty"]       = "Please fill in username and password.",
        ["login.error_credentials"] = "Invalid credentials.",
        ["login.error_connection"]  = "Connection error",
    };
}
