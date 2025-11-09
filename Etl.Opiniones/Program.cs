using Etl.Opiniones;
using ETL.Opiniones.Application.Configuration;
using ETL.Opiniones.Infrastructure.Configuration;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.AddConsole();

builder.Services.AddHostedService<Worker>();

builder.Services.AddApplicationServices();
builder.Services.AddInfrastructureServices(builder.Configuration);
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
