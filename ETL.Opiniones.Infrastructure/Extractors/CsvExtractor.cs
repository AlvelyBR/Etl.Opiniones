

using CsvHelper;
using CsvHelper.Configuration;
using ETL.Opiniones.Application.Interfaces;
using ETL.Opiniones.Application.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Globalization;

namespace ETL.Opiniones.Infrastructure.Extractors
{
    public class CsvExtractor : ICsvExtractor
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<CsvExtractor> _logger;

        public CsvExtractor(IConfiguration configuration, ILogger<CsvExtractor> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        private string GetPath(string key)
        {
            var path = _configuration[key];
            if (string.IsNullOrWhiteSpace(path))
                throw new InvalidOperationException($"No se encontró la ruta para la clave '{key}' en appsettings.json.");
            return path;
        }

        private CsvConfiguration GetCsvConfig()
        {
            return new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                Delimiter = ",",
                Encoding = System.Text.Encoding.UTF8
            };
        }

        // PRODUCTS 
        public async Task<IReadOnlyCollection<RawProduct>> GetProductsAsync(CancellationToken cancellationToken)
        {
            var path = GetPath("DataSources:CsvFiles:Products");
            _logger.LogInformation("Leyendo Products desde {Path}", path);

            using var reader = new StreamReader(path);
            using var csv = new CsvReader(reader, GetCsvConfig());

            var records = new List<RawProduct>();

            if (await csv.ReadAsync())
                csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                var idProducto = csv.GetField<string>("IdProducto");
                var nombre = csv.GetField<string>("Nombre");
                var categoria = csv.GetField<string>("Categoría"); 

                records.Add(new RawProduct
                {
                    IdProducto = idProducto,
                    Nombre = nombre,
                    Categoria = categoria
                });
            }

            _logger.LogInformation("Total Products leídos: {Count}", records.Count);
            return records;
        }

        //  CLIENTS 
        public async Task<IReadOnlyCollection<RawClient>> GetClientsAsync(CancellationToken cancellationToken)
        {
            var path = GetPath("DataSources:CsvFiles:Clients");
            _logger.LogInformation("Leyendo Clients desde {Path}", path);

            using var reader = new StreamReader(path);
            using var csv = new CsvReader(reader, GetCsvConfig());

            var records = new List<RawClient>();

            if (await csv.ReadAsync())
                csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                var idCliente = csv.GetField<string>("IdCliente");
                var nombre = csv.GetField<string>("Nombre");
                var email = csv.GetField<string>("Email");

                records.Add(new RawClient
                {
                    IdCliente = idCliente,
                    Nombre = nombre,
                    Email = email
                });
            }

            _logger.LogInformation("Total Clients leídos: {Count}", records.Count);
            return records;
        }

        // FUENTE_DATOS
        public async Task<IReadOnlyCollection<RawFuenteDatos>> GetFuentesDatosAsync(CancellationToken cancellationToken)
        {
            var path = GetPath("DataSources:CsvFiles:FuenteDatos");
            _logger.LogInformation("Leyendo FuenteDatos desde {Path}", path);

            using var reader = new StreamReader(path);
            using var csv = new CsvReader(reader, GetCsvConfig());

            var records = new List<RawFuenteDatos>();

            if (await csv.ReadAsync())
                csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                var idFuente = csv.GetField<string>("IdFuente");
                var tipoFuente = csv.GetField<string>("TipoFuente");
                var fechaStr = csv.GetField<string>("FechaCarga");

                DateTime? fecha = null;
                if (DateTime.TryParse(fechaStr, out var parsed))
                    fecha = parsed;

                records.Add(new RawFuenteDatos
                {
                    IdFuente = idFuente,
                    TipoFuente = tipoFuente,
                    FechaCarga = fecha
                });
            }

            _logger.LogInformation("Total FuenteDatos leídos: {Count}", records.Count);
            return records;
        }

        //WEB_REVIEWS
        public async Task<IReadOnlyCollection<RawWebReview>> GetWebReviewsAsync(CancellationToken cancellationToken)
        {
            var path = GetPath("DataSources:CsvFiles:WebReviews");
            _logger.LogInformation("Leyendo WebReviews desde {Path}", path);

            using var reader = new StreamReader(path);
            using var csv = new CsvReader(reader, GetCsvConfig());

            var records = new List<RawWebReview>();

            if (await csv.ReadAsync())
                csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                var item = new RawWebReview
                {
                    IdReview = csv.GetField<string>("IdReview"),
                    IdCliente = csv.GetField<string>("IdCliente"),
                    IdProducto = csv.GetField<string>("IdProducto"),
                    Fecha = DateTime.Parse(csv.GetField<string>("Fecha")),
                    Comentario = csv.GetField<string>("Comentario"),
                    Rating = decimal.Parse(csv.GetField<string>("Rating"))
                };

                records.Add(item);
            }

            _logger.LogInformation("Total WebReviews leídos: {Count}", records.Count);
            return records;
        }

        // SURVEYS_PART1
        public async Task<IReadOnlyCollection<RawSurveyOpinion>> GetSurveyOpinionsAsync(CancellationToken cancellationToken)
        {
            var path = GetPath("DataSources:CsvFiles:SurveysPart1");
            _logger.LogInformation("Leyendo SurveyOpinions desde {Path}", path);

            using var reader = new StreamReader(path);
            using var csv = new CsvReader(reader, GetCsvConfig());

            var records = new List<RawSurveyOpinion>();

            if (await csv.ReadAsync())
                csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                var item = new RawSurveyOpinion
                {
                    IdOpinion = csv.GetField<string>("IdOpinion"),
                    IdCliente = csv.GetField<string>("IdCliente"),
                    IdProducto = csv.GetField<string>("IdProducto"),
                    Fecha = DateTime.Parse(csv.GetField<string>("Fecha")),
                    Comentario = csv.GetField<string>("Comentario"),
                    // Ajusta el nombre de la columna según tu CSV real:
                    Clasificacion = csv.GetField<string>("Clasificación"), // o "ClasificaciÃ³n"
                    PuntajeSatisfaccion = decimal.Parse(csv.GetField<string>("PuntajeSatisfacción")), // o el nombre exacto
                    Fuente = csv.GetField<string>("Fuente")
                };

                records.Add(item);
            }

            _logger.LogInformation("Total SurveyOpinions leídos: {Count}", records.Count);
            return records;
        }

        // SOCIAL_COMMENTS
        public async Task<IReadOnlyCollection<RawSocialComment>> GetSocialCommentsAsync(CancellationToken cancellationToken)
        {
            var path = GetPath("DataSources:CsvFiles:SocialComments");
            _logger.LogInformation("Leyendo SocialComments desde {Path}", path);

            using var reader = new StreamReader(path);
            using var csv = new CsvReader(reader, GetCsvConfig());

            var records = new List<RawSocialComment>();

            if (await csv.ReadAsync())
                csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                var item = new RawSocialComment
                {
                    IdComment = csv.GetField<string>("IdComment"),
                    IdCliente = csv.GetField<string>("IdCliente"),
                    IdProducto = csv.GetField<string>("IdProducto"),
                    Fuente = csv.GetField<string>("Fuente"),
                    Fecha = DateTime.Parse(csv.GetField<string>("Fecha")),
                    Comentario = csv.GetField<string>("Comentario")
                };

                records.Add(item);
            }

            _logger.LogInformation("Total SocialComments leídos: {Count}", records.Count);
            return records;
        }
    }
}
