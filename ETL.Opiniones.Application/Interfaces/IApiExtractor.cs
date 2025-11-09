
using ETL.Opiniones.Application.Models;

namespace ETL.Opiniones.Application.Interfaces
{
    public interface IApiExtractor
    {
        Task<IReadOnlyCollection<RawApiOpinion>> GetOpinionsAsync(CancellationToken cancellationToken);
    }
}
