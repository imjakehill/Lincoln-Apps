namespace SupplyOn_Blazor.Services;

public enum TipoNotificacion { Exito, Error }

public class NotificationService
{
    public string? Mensaje { get; private set; }
    public TipoNotificacion Tipo { get; private set; }
    public bool Activa { get; private set; } = false;

    public void Mostrar(string mensaje, TipoNotificacion tipo)
    {
        Mensaje = mensaje;
        Tipo = tipo;
        Activa = true;
    }

    public void Limpiar()
    {
        Mensaje = null;
        Activa = false;
    }
}