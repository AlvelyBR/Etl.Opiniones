

using ETL.Opiniones.Application.Interfaces;
using ETL.Opiniones.Application.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ETL.Opiniones.Infrastructure.Extractors
{
    public class DatabaseExtractor : IDatabaseExtractor
    {
        private readonly string _connectionString;
        private readonly ILogger<DatabaseExtractor> _logger;

        public DatabaseExtractor(IConfiguration configuration, ILogger<DatabaseExtractor> logger)
        {
            _connectionString = configuration["DataSources:RelationalDatabase:ConnectionString"]
                                ?? throw new InvalidOperationException("ConnectionString no encontrada para la base relacional.");
            _logger = logger;
        }

        public async Task<IReadOnlyCollection<RawDbOpinion>> GetOpinionesAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Leyendo opiniones desde la base de datos relacional 'Opiniones'...");

            var result = new List<RawDbOpinion>();

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
                SELECT 
                    o.idopinion,
                    c.idcliente,
                    c.nombre AS cliente_nombre,
                    c.email AS cliente_email,
                    p.idproducto,
                    p.nombre AS producto_nombre,
                    cp.nombre AS categoria_nombre,
                    o.fecha,
                    o.comentario,
                    o.puntajesatisfaccion,
                    o.rating,
                    o.idfuente,
                    tf.nombre AS tipo_fuente,
                    o.tipo_opinion,
                    o.fuente_social,
                    cl.nombre AS clasificacion_nombre
                FROM ""Opiniones"".opiniones o
                JOIN ""Opiniones"".clientes c ON c.idcliente = o.cliente
                JOIN ""Opiniones"".productos p ON p.idproducto = o.producto
                LEFT JOIN ""Opiniones"".categoriasproductos cp ON cp.idcategoria = p.categoria
                LEFT JOIN ""Opiniones"".clasificaciones cl ON cl.idclasificacion = o.idclasificacion
                JOIN ""Opiniones"".fuentesdatos fd ON fd.idfuentedato = o.idfuente
                LEFT JOIN ""Opiniones"".tipofuente tf ON tf.idtipofuente = fd.tipofuente;
            ";

            await using var cmd = new NpgsqlCommand(sql, connection);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                var opinion = new RawDbOpinion
                {
                    IdOpinion = reader.GetInt32(reader.GetOrdinal("idopinion")),

                    IdCliente = reader.GetInt32(reader.GetOrdinal("idcliente")),
                    ClienteNombre = reader.GetString(reader.GetOrdinal("cliente_nombre")),
                    ClienteEmail = reader.GetString(reader.GetOrdinal("cliente_email")),

                    IdProducto = reader.GetString(reader.GetOrdinal("idproducto")),
                    ProductoNombre = reader.GetString(reader.GetOrdinal("producto_nombre")),
                    CategoriaNombre = reader.IsDBNull(reader.GetOrdinal("categoria_nombre"))
                        ? null
                        : reader.GetString(reader.GetOrdinal("categoria_nombre")),

                    Fecha = reader.GetDateTime(reader.GetOrdinal("fecha")),
                    Comentario = reader.IsDBNull(reader.GetOrdinal("comentario"))
                        ? null
                        : reader.GetString(reader.GetOrdinal("comentario")),

                    PuntajeSatisfaccion = reader.IsDBNull(reader.GetOrdinal("puntajesatisfaccion"))
                        ? (int?)null
                        : reader.GetInt32(reader.GetOrdinal("puntajesatisfaccion")),

                    Rating = reader.IsDBNull(reader.GetOrdinal("rating"))
                        ? (int?)null
                        : reader.GetInt32(reader.GetOrdinal("rating")),

                    IdFuente = reader.GetInt32(reader.GetOrdinal("idfuente")),
                    TipoFuente = reader.IsDBNull(reader.GetOrdinal("tipo_fuente"))
                        ? null
                        : reader.GetString(reader.GetOrdinal("tipo_fuente")),

                    TipoOpinion = reader.GetString(reader.GetOrdinal("tipo_opinion")),

                    FuenteSocial = reader.IsDBNull(reader.GetOrdinal("fuente_social"))
                        ? null
                        : reader.GetString(reader.GetOrdinal("fuente_social")),

                    ClasificacionNombre = reader.IsDBNull(reader.GetOrdinal("clasificacion_nombre"))
                        ? null
                        : reader.GetString(reader.GetOrdinal("clasificacion_nombre"))
                };

                result.Add(opinion);
            }

            _logger.LogInformation("Total opiniones leídas desde la base relacional: {Count}", result.Count);

            return result;
        }
    }
}
