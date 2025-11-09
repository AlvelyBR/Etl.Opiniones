using ETL.Opiniones.Application.Interfaces;
using ETL.Opiniones.Infrastructure.DataLoaders;
using ETL.Opiniones.Infrastructure.Extractors;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace ETL.Opiniones.Infrastructure.Configuration
{
    public static class InfrastructureServiceRegistration
    {
        public static IServiceCollection AddInfrastructureServices(
        this IServiceCollection services,
        IConfiguration configuration)
        {
            

            // HttpClient para API
            services.AddHttpClient();

            // Extractores
            services.AddTransient<ICsvExtractor, CsvExtractor>();
            services.AddTransient<IDatabaseExtractor, DatabaseExtractor>();
            services.AddTransient<IApiExtractor, ApiExtractor>();

            // Loader hacia el DW
            services.AddTransient<IDataLoader, DataLoader>();

            return services;
        }
}
}
