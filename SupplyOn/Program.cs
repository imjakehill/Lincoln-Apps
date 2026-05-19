using SupplyOn_Blazor.Components;
using SupplyOn_Blazor.Services;

var builder = WebApplication.CreateBuilder(args);

// Servicios
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.WebHost.UseUrls("http://0.0.0.0:5085");
builder.Services.AddScoped<SesionService>();
builder.Services.AddScoped<NotificationService>();
builder.Services.AddScoped<BasApiClient>();
builder.Services.AddScoped<RequerimientoService>();
builder.Services.AddScoped<LanguageService>();
builder.Services.AddHttpClient();

// Build
var app = builder.Build();

// Pipeline 
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapGet("/", context =>
{
    context.Response.Redirect("/login");
    return Task.CompletedTask;
});

app.Run();