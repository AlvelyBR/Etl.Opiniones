

namespace ETL.Opiniones.Application.Interfaces
{
    public interface IExtractionOrchestrator
    {
        Task ExecuteAsync(CancellationToken cancellationToken);
    }
}
