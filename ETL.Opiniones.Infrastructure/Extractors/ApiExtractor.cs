

using ETL.Opiniones.Application.Interfaces;
using ETL.Opiniones.Application.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using System.Text.Json;
using ETL.Opiniones.Infrastructure.Dtos;



namespace ETL.Opiniones.Infrastructure.Extractors
{
    public class ApiExtractor : IApiExtractor
    {
        private readonly IHttpClientFactory _clientFactory;
        private readonly IConfiguration _configuration;
        private readonly ILogger<ApiExtractor> _logger;

        public ApiExtractor(
            IHttpClientFactory clientFactory,
            IConfiguration configuration,
            ILogger<ApiExtractor> logger)
        {
            _clientFactory = clientFactory;
            _configuration = configuration;
            _logger = logger;
        }

        public async Task<IReadOnlyCollection<RawApiOpinion>> GetOpinionsAsync(CancellationToken cancellationToken)
        {
            var baseUrl = _configuration["DataSources:Api:BaseUrl"];
            var endpoint = _configuration["DataSources:Api:CommentsEndpoint"];
            var apiKey = _configuration["DataSources:Api:ApiKey"];

            if (string.IsNullOrWhiteSpace(baseUrl) || string.IsNullOrWhiteSpace(endpoint))
            {
                _logger.LogWarning("Configuración de API REST incompleta. Se omite la extracción desde API.");
                return Array.Empty<RawApiOpinion>();
            }

            try
            {
                var client = _clientFactory.CreateClient();
                client.BaseAddress = new Uri(baseUrl);

                if (!string.IsNullOrWhiteSpace(apiKey))
                {
                    client.DefaultRequestHeaders.Add("Authorization", $"Bearer {apiKey}");
                }

                _logger.LogInformation("Consumiendo API REST de opiniones en {Url}{Endpoint}", baseUrl, endpoint);

                var response = await client.GetAsync(endpoint, cancellationToken);

                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning("API REST devolvió código {StatusCode}. Se omite carga desde API.", response.StatusCode);
                    return Array.Empty<RawApiOpinion>();
                }

                var content = await response.Content.ReadAsStringAsync(cancellationToken);

                var options = new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                };

                
                var apiItems = JsonSerializer.Deserialize<List<ApiOpinionDto>>(content, options)
                               ?? new List<ApiOpinionDto>();

                var result = apiItems
                    .Select(x => new RawApiOpinion
                    {
                        Id = x.Id ?? x.IdOpinion ?? Guid.NewGuid().ToString(), 
                        IdCliente = x.IdCliente,
                        IdProducto = x.IdProducto,
                        Fecha = x.Fecha,
                        Comentario = x.Comentario ?? string.Empty,
                        Puntuacion = x.Puntuacion
                    })
                    .ToList();

                _logger.LogInformation("Total opiniones obtenidas desde API REST: {Count}", result.Count);

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al consumir la API REST de opiniones. Se omite esta fuente.");
                return Array.Empty<RawApiOpinion>();
            }
        }

        
    }
}
