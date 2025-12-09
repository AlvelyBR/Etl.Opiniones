using ETL.Opiniones.Application.Interfaces;
using ETL.Opiniones.Application.Models;
using ETL.Opiniones.Domain.Entities;
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

        public async Task LoadClasificacionesAsync(IEnumerable<DimClasificacion> clasificaciones, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando {Count} clasificaciones en dim_clasificacion...", clasificaciones.Count());

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
            INSERT INTO dimension.dim_clasificacion (nombre)
            VALUES (@Nombre)
            ON CONFLICT (nombre) DO NOTHING;
        ";

            int count = 0;
            foreach (var clasificacion in clasificaciones)
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("@Nombre", clasificacion.Nombre);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
                count++;
            }

            _logger.LogInformation("✓ Cargadas {Count} clasificaciones en dim_clasificacion", count);
        }

        // MÉTODO 2: Redes Sociales
        public async Task LoadRedesSocialesAsync(IEnumerable<DimRedSocial> redesSociales, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando {Count} redes sociales en dim_red_social...", redesSociales.Count());

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
            INSERT INTO dimension.dim_red_social (nombre, fuente_key)
            VALUES (@Nombre, @FuenteKey)
            ON CONFLICT (nombre) DO UPDATE SET
                fuente_key = EXCLUDED.fuente_key;
        ";

            int count = 0;
            foreach (var redSocial in redesSociales)
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("@Nombre", redSocial.Nombre);
                cmd.Parameters.AddWithValue("@FuenteKey", redSocial.FuenteKey);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
                count++;
            }

            _logger.LogInformation("✓ Cargadas {Count} redes sociales en dim_red_social", count);
        }

        // MÉTODO 3: Dimensión de Tiempo
        public async Task LoadDimTimeAsync(IEnumerable<DimTime> tiempos, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Cargando {Count} dimensiones de tiempo en dim_time...", tiempos.Count());

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
            INSERT INTO dimension.dim_time 
            (time_key, date, year, quarter, month, month_nombre, day, day_nombre, week_of_year, is_weekend)
            VALUES (@TimeKey, @Date, @Year, @Quarter, @Month, @MonthNombre, @Day, @DayNombre, @WeekOfYear, @IsWeekend)
            ON CONFLICT (time_key) DO NOTHING;
        ";

            int count = 0;
            foreach (var tiempo in tiempos)
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("@TimeKey", tiempo.TimeKey);
                cmd.Parameters.AddWithValue("@Date", tiempo.Date);
                cmd.Parameters.AddWithValue("@Year", tiempo.Year);
                cmd.Parameters.AddWithValue("@Quarter", tiempo.Quarter);
                cmd.Parameters.AddWithValue("@Month", tiempo.Month);
                cmd.Parameters.AddWithValue("@MonthNombre", tiempo.MonthNombre);
                cmd.Parameters.AddWithValue("@Day", tiempo.Day);
                cmd.Parameters.AddWithValue("@DayNombre", tiempo.DayNombre);
                cmd.Parameters.AddWithValue("@WeekOfYear", tiempo.WeekOfYear);
                cmd.Parameters.AddWithValue("@IsWeekend", tiempo.IsWeekend);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
                count++;
            }

            _logger.LogInformation("✓ Cargadas {Count} dimensiones de tiempo en dim_time", count);
        }

        // MÉTODO 4: Obtener fuentes existentes
        public async Task<List<(long FuenteKey, string IdFuente)>> GetFuentesExistentesAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Obteniendo fuentes existentes de dim_fuente_datos...");

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = "SELECT fuentekey, id_fuente FROM dimension.dim_fuente_datos";

            var fuentes = new List<(long, string)>();
            await using var cmd = new NpgsqlCommand(sql, connection);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync())
            {
                fuentes.Add((reader.GetInt64(0), reader.GetString(1)));
            }

            _logger.LogInformation("✓ Obtenidas {Count} fuentes del DW", fuentes.Count);
            return fuentes;
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
            _logger.LogInformation("Cargando {Count} SurveyOpinions...", opinions.Count());

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string sql = @"
        INSERT INTO fact.fact_opiniones 
        (time_key, producto_key, cliente_key, fuente_key, red_social_key, 
         clasificacion_key, origen, id_origen, puntuacion, comentario, 
         tiene_texto, fecha_opinion)
        VALUES (
            (SELECT time_key FROM dimension.dim_time WHERE date = @Fecha::date),
            (SELECT producto_key FROM dimension.dim_productos WHERE id_producto = @IdProducto::integer),
            (SELECT clientekey FROM dimension.dim_clientes WHERE nk_cliente = @IdCliente),
            (SELECT fuentekey FROM dimension.dim_fuente_datos WHERE id_fuente = 'WEB'), -- O la fuente correcta
            NULL, -- red_social_key como NULL
            (SELECT clasificacionkey FROM dimension.dim_clasificacion WHERE nombre = @Clasificacion),
            'SURVEY',
            @IdOpinion,
            @PuntajeSatisfaccion,
            @Comentario,
            @TieneTexto,
            @Fecha
        )
        ON CONFLICT (origen, id_origen) DO NOTHING;
    ";

            int count = 0;
            foreach (var opinion in opinions)
            {
                try
                {
                    await using var cmd = new NpgsqlCommand(sql, connection);
                    cmd.Parameters.AddWithValue("@Fecha", opinion.Fecha);
                    cmd.Parameters.AddWithValue("@IdProducto", int.Parse(opinion.IdProducto));
                    cmd.Parameters.AddWithValue("@IdCliente", opinion.IdCliente);
                    cmd.Parameters.AddWithValue("@Clasificacion", opinion.Clasificacion);
                    cmd.Parameters.AddWithValue("@IdOpinion", opinion.IdOpinion);
                    cmd.Parameters.AddWithValue("@PuntajeSatisfaccion", opinion.PuntajeSatisfaccion);
                    cmd.Parameters.AddWithValue("@Comentario", opinion.Comentario ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@TieneTexto", !string.IsNullOrWhiteSpace(opinion.Comentario));

                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                    count++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error insertando opinión {Id}", opinion.IdOpinion);
                }
            }

            _logger.LogInformation("✓ Cargadas {Count} SurveyOpinions", count);
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
