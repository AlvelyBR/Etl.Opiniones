using ETL.Opiniones.Application.Interfaces;
using ETL.Opiniones.Application.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ETL.Opiniones.Infrastructure.DataLoaders
{
    public class DataLoader : IDataLoader
    {
        private readonly string _connectionString;
        private readonly ILogger<DataLoader> _logger;

        public DataLoader(IConfiguration configuration, ILogger<DataLoader> logger)
        {
            _connectionString = configuration["DataSources:DataWarehouse:ConnectionString"]
                                ?? throw new InvalidOperationException("ConnectionString no encontrada para el Data Warehouse.");
            _logger = logger;
        }

        public async Task LoadProductsAsync(IEnumerable<RawProduct> products, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando Products en dim_productos...");

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
        INSERT INTO dimension.dim_productos (id_producto, nombre, nombre_categoria)
        VALUES (@id_producto, @nombre, @categoria)
        ON CONFLICT (id_producto) DO NOTHING;";

            foreach (var p in products)
            {
                // Convertir IdProducto tipo 'P016' -> 16
                var raw = p.IdProducto?.Trim();

                if (string.IsNullOrWhiteSpace(raw))
                {
                    _logger.LogWarning("Producto con IdProducto vacío. Se omite.");
                    continue;
                }

                // Quitar prefijos no numéricos (P, etc.)
                var numericPart = new string(raw.Where(char.IsDigit).ToArray());

                if (!int.TryParse(numericPart, out var idProductoInt))
                {
                    _logger.LogWarning("No se pudo convertir IdProducto '{IdProducto}' a entero. Se omite.", p.IdProducto);
                    continue;
                }

                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("id_producto", idProductoInt);
                cmd.Parameters.AddWithValue("nombre", p.Nombre);
                cmd.Parameters.AddWithValue("categoria", p.Categoria);

                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            _logger.LogInformation("Carga de Products completada.");
        }

        public async Task LoadClientsAsync(IEnumerable<RawClient> clients, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando Clients en dim_clientes...");

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
                INSERT INTO dimension.dim_clientes (nk_cliente, nombre, email)
                VALUES (@nk_cliente, @nombre, @email)
                ON CONFLICT (nk_cliente) DO NOTHING;";

            foreach (var c in clients)
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("nk_cliente", c.IdCliente);
                cmd.Parameters.AddWithValue("nombre", c.Nombre);
                cmd.Parameters.AddWithValue("email", c.Email);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            _logger.LogInformation("Carga de Clients completada.");
        }

        public async Task LoadFuentesDatosAsync(IEnumerable<RawFuenteDatos> fuentes, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando FuenteDatos en dim_fuente_datos...");

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
                INSERT INTO dimension.dim_fuente_datos (id_fuente, tipo_fuente, fecha_carga)
                VALUES (@id_fuente, @tipo_fuente, @fecha_carga)
                ON CONFLICT (id_fuente) DO NOTHING;";

            foreach (var f in fuentes)
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("id_fuente", f.IdFuente);
                cmd.Parameters.AddWithValue("tipo_fuente", f.TipoFuente);
                cmd.Parameters.AddWithValue("fecha_carga", (object?)f.FechaCarga ?? DBNull.Value);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            _logger.LogInformation("Carga de FuenteDatos completada.");
        }

        public async Task LoadWebReviewsAsync(IEnumerable<RawWebReview> reviews, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando WebReviews en fact_opiniones (modo extracción)...");

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
                INSERT INTO fact.fact_opiniones (
                    origen, id_origen, comentario, puntuacion, tiene_texto, fecha_opinion
                )
                VALUES (@origen, @id_origen, @comentario, @puntuacion, @tiene_texto, @fecha_opinion)
                ON CONFLICT (origen, id_origen) DO NOTHING;";

            foreach (var r in reviews)
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("origen", "web_reviews");
                cmd.Parameters.AddWithValue("id_origen", r.IdReview);
                cmd.Parameters.AddWithValue("comentario", (object?)r.Comentario ?? DBNull.Value);
                cmd.Parameters.AddWithValue("puntuacion", r.Rating);
                cmd.Parameters.AddWithValue("tiene_texto", !string.IsNullOrWhiteSpace(r.Comentario));
                cmd.Parameters.AddWithValue("fecha_opinion", r.Fecha);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            _logger.LogInformation("Carga de WebReviews completada.");
        }

        public async Task LoadSurveyOpinionsAsync(IEnumerable<RawSurveyOpinion> opinions, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando SurveyOpinions en fact_opiniones (modo extracción)...");

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
                INSERT INTO fact.fact_opiniones (
                    origen, id_origen, comentario, puntuacion, tiene_texto, fecha_opinion
                )
                VALUES (@origen, @id_origen, @comentario, @puntuacion, @tiene_texto, @fecha_opinion)
                ON CONFLICT (origen, id_origen) DO NOTHING;";

            foreach (var o in opinions)
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("origen", "surveys_part1");
                cmd.Parameters.AddWithValue("id_origen", o.IdOpinion);
                cmd.Parameters.AddWithValue("comentario", (object?)o.Comentario ?? DBNull.Value);
                cmd.Parameters.AddWithValue("puntuacion", o.PuntajeSatisfaccion);
                cmd.Parameters.AddWithValue("tiene_texto", !string.IsNullOrWhiteSpace(o.Comentario));
                cmd.Parameters.AddWithValue("fecha_opinion", o.Fecha);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            _logger.LogInformation("Carga de SurveyOpinions completada.");
        }

        public async Task LoadSocialCommentsAsync(IEnumerable<RawSocialComment> comments, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando SocialComments en fact_opiniones (modo extracción)...");

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
                INSERT INTO fact.fact_opiniones (
                    origen, id_origen, comentario, tiene_texto, fecha_opinion
                )
                VALUES (@origen, @id_origen, @comentario, @tiene_texto, @fecha_opinion)
                ON CONFLICT (origen, id_origen) DO NOTHING;";

            foreach (var c in comments)
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("origen", "social_comments");
                cmd.Parameters.AddWithValue("id_origen", c.IdComment);
                cmd.Parameters.AddWithValue("comentario", (object?)c.Comentario ?? DBNull.Value);
                cmd.Parameters.AddWithValue("tiene_texto", !string.IsNullOrWhiteSpace(c.Comentario));
                cmd.Parameters.AddWithValue("fecha_opinion", c.Fecha);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            _logger.LogInformation("Carga de SocialComments completada.");
        }

        public async Task LoadDatabaseOpinionsAsync(IEnumerable<RawDbOpinion> opinions, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando Opiniones desde la BD relacional en fact_opiniones...");

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
        INSERT INTO fact.fact_opiniones (
            origen, id_origen, comentario, puntuacion, tiene_texto, fecha_opinion
        )
        VALUES (@origen, @id_origen, @comentario, @puntuacion, @tiene_texto, @fecha_opinion)
        ON CONFLICT (origen, id_origen) DO NOTHING;";

            foreach (var o in opinions)
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("origen", "db_relacional");
                cmd.Parameters.AddWithValue("id_origen", o.IdOpinion.ToString());
                cmd.Parameters.AddWithValue("comentario", (object?)o.Comentario ?? DBNull.Value);
                cmd.Parameters.AddWithValue("puntuacion", (object?)o.Rating ?? (object?)o.PuntajeSatisfaccion ?? DBNull.Value);
                cmd.Parameters.AddWithValue("tiene_texto", !string.IsNullOrWhiteSpace(o.Comentario));
                cmd.Parameters.AddWithValue("fecha_opinion", o.Fecha);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            _logger.LogInformation("Carga de Opiniones desde la BD relacional completada.");
        }
        public async Task LoadApiOpinionsAsync(IEnumerable<RawApiOpinion> opinions, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando opiniones desde API REST en fact_opiniones...");

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
        INSERT INTO fact.fact_opiniones (
            origen, id_origen, comentario, puntuacion, tiene_texto, fecha_opinion
        )
        VALUES (@origen, @id_origen, @comentario, @puntuacion, @tiene_texto, @fecha_opinion)
        ON CONFLICT (origen, id_origen) DO NOTHING;";

            foreach (var o in opinions)
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("origen", "api_rest");
                cmd.Parameters.AddWithValue("id_origen", o.Id);
                cmd.Parameters.AddWithValue("comentario", (object?)o.Comentario ?? DBNull.Value);
                cmd.Parameters.AddWithValue("puntuacion", (object?)o.Puntuacion ?? DBNull.Value);
                cmd.Parameters.AddWithValue("tiene_texto", !string.IsNullOrWhiteSpace(o.Comentario));
                cmd.Parameters.AddWithValue("fecha_opinion", o.Fecha);

                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            _logger.LogInformation("Carga de opiniones desde API REST completada.");
        }
    }
}
