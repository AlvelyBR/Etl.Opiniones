

using ETL.Opiniones.Application.Models;

namespace ETL.Opiniones.Application.Interfaces
{
    public interface ICsvExtractor
    {
        Task<IReadOnlyCollection<RawWebReview>> GetWebReviewsAsync(CancellationToken cancellationToken);
        Task<IReadOnlyCollection<RawSurveyOpinion>> GetSurveyOpinionsAsync(CancellationToken cancellationToken);
        Task<IReadOnlyCollection<RawSocialComment>> GetSocialCommentsAsync(CancellationToken cancellationToken);
        Task<IReadOnlyCollection<RawProduct>> GetProductsAsync(CancellationToken cancellationToken);
        Task<IReadOnlyCollection<RawFuenteDatos>> GetFuentesDatosAsync(CancellationToken cancellationToken);
        Task<IReadOnlyCollection<RawClient>> GetClientsAsync(CancellationToken cancellationToken);
    }
}
