namespace SupplyOn_Blazor.Services;

public class SesionService
{
    public bool isAuthenticated { get; private set; } = false;
    public string userName { get; private set; } = string.Empty;
    public string AccessToken { get; private set; } = string.Empty; // ← nuevo

    public void logInUser(string usuario, string token)  // ← recibe el token
    {
        isAuthenticated = true;
        userName = usuario;
        AccessToken = token;
    }

    public void logOutUser()
    {
        isAuthenticated = false;
        userName = string.Empty;
        AccessToken = string.Empty;
    }
}
