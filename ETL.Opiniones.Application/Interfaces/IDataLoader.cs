

using ETL.Opiniones.Application.Models;
using ETL.Opiniones.Domain.Entities;

namespace ETL.Opiniones.Application.Interfaces
{
    public interface IDataLoader
    {
        Task LoadWebReviewsAsync(IEnumerable<RawWebReview> reviews, CancellationToken cancellationToken);
        Task LoadSurveyOpinionsAsync(IEnumerable<RawSurveyOpinion> opinions, CancellationToken cancellationToken);
        Task LoadSocialCommentsAsync(IEnumerable<RawSocialComment> comments, CancellationToken cancellationToken);
        Task LoadProductsAsync(IEnumerable<RawProduct> products, CancellationToken cancellationToken);
        Task LoadClasificacionesAsync(IEnumerable<DimClasificacion> clasificaciones, CancellationToken cancellationToken);
        Task LoadRedesSocialesAsync(IEnumerable<DimRedSocial> redesSociales, CancellationToken cancellationToken);
        Task LoadDimTimeAsync(IEnumerable<DimTime> tiempos, CancellationToken cancellationToken);
        Task LoadFuentesDatosAsync(IEnumerable<RawFuenteDatos> fuentes, CancellationToken cancellationToken);
        Task LoadClientsAsync(IEnumerable<RawClient> clients, CancellationToken cancellationToken);
        Task LoadDatabaseOpinionsAsync(IEnumerable<RawDbOpinion> opinions, CancellationToken cancellationToken);
        Task LoadApiOpinionsAsync(IEnumerable<RawApiOpinion> opinions, CancellationToken cancellationToken);
        Task<List<(long FuenteKey, string IdFuente)>> GetFuentesExistentesAsync(CancellationToken cancellationToken);

    }
}
