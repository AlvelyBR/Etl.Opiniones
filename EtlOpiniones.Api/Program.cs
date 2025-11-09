using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);

// servicios al contenedor
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "Etl Opiniones API",
        Version = "v1",
        Description = "API de comentarios para pruebas del proceso ETL"
    });
});

var app = builder.Build();

// Configurar el pipeline HTTP
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "Etl Opiniones API v1");
        c.RoutePrefix = string.Empty; // <--  (https://localhost:7011)
    });
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
