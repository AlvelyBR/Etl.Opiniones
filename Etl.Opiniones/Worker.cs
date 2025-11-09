

using ETL.Opiniones.Application.Interfaces;

namespace Etl.Opiniones
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IExtractionOrchestrator _orchestrator;

        public Worker(ILogger<Worker> logger, IExtractionOrchestrator orchestrator)
        {
            _logger = logger;
            _orchestrator = orchestrator;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker iniciado. Ejecutando proceso ETL de extracción...");

            try
            {
                await _orchestrator.ExecuteAsync(stoppingToken);
                _logger.LogInformation("Proceso ETL de extracción completado correctamente.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error durante la ejecución del proceso ETL.");
            }


            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}
