
using ETL.Opiniones.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace ETL.Opiniones.Application.Services
{
    public class ExtractionOrchestrator : IExtractionOrchestrator
    {
        private readonly ICsvExtractor _csvExtractor;
        private readonly IDataLoader _dataLoader;
        private readonly ILogger<ExtractionOrchestrator> _logger;
        private readonly IDatabaseExtractor _databaseExtractor;
        private readonly IApiExtractor _apiExtractor;

        public ExtractionOrchestrator(
            ICsvExtractor csvExtractor,
            IDataLoader dataLoader,
            IDatabaseExtractor databaseExtractor,
            IApiExtractor apiExtractor,
            ILogger<ExtractionOrchestrator> logger)
        {
            _csvExtractor = csvExtractor;
            _dataLoader = dataLoader;
            _databaseExtractor = databaseExtractor;
            _apiExtractor = apiExtractor;
            _logger = logger;
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Inicio del proceso ETL de extracción...");

            var products = await _csvExtractor.GetProductsAsync(cancellationToken);
            await _dataLoader.LoadProductsAsync(products, cancellationToken);

            var clients = await _csvExtractor.GetClientsAsync(cancellationToken);
            await _dataLoader.LoadClientsAsync(clients, cancellationToken);

            var fuentes = await _csvExtractor.GetFuentesDatosAsync(cancellationToken);
            await _dataLoader.LoadFuentesDatosAsync(fuentes, cancellationToken);

            var webReviews = await _csvExtractor.GetWebReviewsAsync(cancellationToken);
            await _dataLoader.LoadWebReviewsAsync(webReviews, cancellationToken);

            var surveys = await _csvExtractor.GetSurveyOpinionsAsync(cancellationToken);
            await _dataLoader.LoadSurveyOpinionsAsync(surveys, cancellationToken);

            var social = await _csvExtractor.GetSocialCommentsAsync(cancellationToken);
            await _dataLoader.LoadSocialCommentsAsync(social, cancellationToken);

            _logger.LogInformation("Extrayendo opiniones desde la base de datos relacional...");
            var dbOpinions = await _databaseExtractor.GetOpinionesAsync(cancellationToken);
            await _dataLoader.LoadDatabaseOpinionsAsync(dbOpinions, cancellationToken);

            _logger.LogInformation("Extrayendo opiniones desde API REST...");
            var apiOpinions = await _apiExtractor.GetOpinionsAsync(cancellationToken);
            await _dataLoader.LoadApiOpinionsAsync(apiOpinions, cancellationToken);

            _logger.LogInformation("Proceso ETL de extracción completado con éxito.");
        }
    }
}
