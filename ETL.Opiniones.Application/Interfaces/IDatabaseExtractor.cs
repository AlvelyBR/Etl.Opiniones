

using ETL.Opiniones.Application.Models;

namespace ETL.Opiniones.Application.Interfaces
{
    public interface IDatabaseExtractor
    {
        Task<IReadOnlyCollection<RawDbOpinion>> GetOpinionesAsync(CancellationToken cancellationToken);
    }
}
