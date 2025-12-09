using ETL.Opiniones.Application.Interfaces;
using ETL.Opiniones.Domain.Entities;
using Microsoft.Extensions.Logging;
using System.Globalization;

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

            // 1. CARGAR TODAS LAS DIMENSIONES PRIMERO

            // Productos
            var products = await _csvExtractor.GetProductsAsync(cancellationToken);
            await _dataLoader.LoadProductsAsync(products, cancellationToken);

            // Clientes
            var clients = await _csvExtractor.GetClientsAsync(cancellationToken);
            await _dataLoader.LoadClientsAsync(clients, cancellationToken);

            // Fuentes de datos
            var fuentes = await _csvExtractor.GetFuentesDatosAsync(cancellationToken);
            await _dataLoader.LoadFuentesDatosAsync(fuentes, cancellationToken);

            // Clasificaciones - primero extraer surveys para obtener las clasificaciones
            var surveys = await _csvExtractor.GetSurveyOpinionsAsync(cancellationToken);

            var clasificacionesUnicas = surveys
               .Select(s => s.Clasificacion)
               .Where(c => !string.IsNullOrWhiteSpace(c))
               .Distinct()
               .Select(c => new DimClasificacion { Nombre = c })
               .ToList();

            // Agregar las básicas por si faltan
            var basicas = new[] { "Positiva", "Negativa", "Neutra" };
            foreach (var basica in basicas)
            {
                if (!clasificacionesUnicas.Any(c => c.Nombre == basica))
                {
                    clasificacionesUnicas.Add(new DimClasificacion { Nombre = basica });
                }
            }
            await _dataLoader.LoadClasificacionesAsync(clasificacionesUnicas, cancellationToken);

            // Redes sociales
            // Necesitamos obtener las fuentes del DW primero
            var fuentesDW = await _dataLoader.GetFuentesExistentesAsync(cancellationToken);
            var redesSociales = new List<DimRedSocial>();

            foreach (var (fuenteKey, idFuente) in fuentesDW)
            {
                var redSocial = MapFuenteToRedSocial(idFuente);
                if (redSocial != null)
                {
                    redesSociales.Add(new DimRedSocial
                    {
                        Nombre = redSocial,
                        FuenteKey = fuenteKey
                    });
                }
            }
            await _dataLoader.LoadRedesSocialesAsync(redesSociales, cancellationToken);

            // Dimensión de tiempo - recolectar todas las fechas
            var todasLasFechas = new List<DateTime>();

            // Fechas de surveys
            todasLasFechas.AddRange(surveys.Select(s => s.Fecha.Date));

            // Web reviews
            var webReviews = await _csvExtractor.GetWebReviewsAsync(cancellationToken);
            todasLasFechas.AddRange(webReviews.Select(w => w.Fecha.Date));

            // Social comments
            var social = await _csvExtractor.GetSocialCommentsAsync(cancellationToken);
            todasLasFechas.AddRange(social.Select(s => s.Fecha.Date));

            // DB opinions
            var dbOpinions = await _databaseExtractor.GetOpinionesAsync(cancellationToken);
            todasLasFechas.AddRange(dbOpinions.Select(d => d.Fecha.Date));

            // API opinions
            var apiOpinions = await _apiExtractor.GetOpinionsAsync(cancellationToken);
            if (apiOpinions.Any())
            {
                todasLasFechas.AddRange(apiOpinions.Select(a => a.Fecha.Date));
            }

            // Generar y cargar dimensión de tiempo
            var fechasUnicas = todasLasFechas.Distinct().ToList();
            var tiempos = GenerateDimTime(fechasUnicas);
            await _dataLoader.LoadDimTimeAsync(tiempos, cancellationToken);

            // 2. AHORA CARGAR LOS HECHOS (FACT TABLE)

            await _dataLoader.LoadWebReviewsAsync(webReviews, cancellationToken);
            await _dataLoader.LoadSurveyOpinionsAsync(surveys, cancellationToken);
            await _dataLoader.LoadSocialCommentsAsync(social, cancellationToken);
            await _dataLoader.LoadDatabaseOpinionsAsync(dbOpinions, cancellationToken);

            if (apiOpinions.Any())
            {
                await _dataLoader.LoadApiOpinionsAsync(apiOpinions, cancellationToken);
            }

            _logger.LogInformation("Proceso ETL de extracción completado con éxito.");
        }

        private string? MapFuenteToRedSocial(string idFuente)
        {
            return idFuente.ToUpper() switch
            {
                "FB" or "FACEBOOK" => "Facebook",
                "TW" or "TWITTER" => "Twitter",
                "IG" or "INSTAGRAM" => "Instagram",
                "ENCUESTAINTERNA" => "Encuesta Interna",
                "WEB" => "Sitio Web",
                _ => null
            };
        }

        private List<DimTime> GenerateDimTime(List<DateTime> fechas)
        {
            if (!fechas.Any())
            {
                _logger.LogWarning("No se encontraron fechas. Generando rango por defecto.");
                // Generar rango por defecto
                var defaultStart = DateTime.Today.AddYears(-1);
                var defaultEnd = DateTime.Today.AddMonths(1);

                fechas = new List<DateTime>();
                for (var date = defaultStart; date <= defaultEnd; date = date.AddDays(1))
                {
                    fechas.Add(date);
                }
            }

            var minDate = fechas.Min().AddDays(-30);
            var maxDate = fechas.Max().AddDays(30);

            var tiempos = new List<DimTime>();

            for (var date = minDate; date <= maxDate; date = date.AddDays(1))
            {
                var timeKey = int.Parse(date.ToString("yyyyMMdd"));
                var dayOfWeek = date.DayOfWeek;

                tiempos.Add(new DimTime
                {
                    TimeKey = timeKey,
                    Date = date,
                    Year = date.Year,
                    Quarter = (date.Month - 1) / 3 + 1,
                    Month = date.Month,
                    MonthNombre = date.ToString("MMMM", CultureInfo.GetCultureInfo("es-ES")),
                    Day = date.Day,
                    DayNombre = dayOfWeek.ToString(),
                    WeekOfYear = CultureInfo.CurrentCulture.Calendar.GetWeekOfYear(
                        date, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday),
                    IsWeekend = dayOfWeek == DayOfWeek.Saturday || dayOfWeek == DayOfWeek.Sunday
                });
            }

            _logger.LogInformation("Generadas {Count} dimensiones de tiempo desde {MinDate} hasta {MaxDate}",
                tiempos.Count, minDate.ToString("yyyy-MM-dd"), maxDate.ToString("yyyy-MM-dd"));

            return tiempos;
        }
    }
}