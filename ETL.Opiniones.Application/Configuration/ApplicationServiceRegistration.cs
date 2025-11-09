

using ETL.Opiniones.Application.Interfaces;
using ETL.Opiniones.Application.Services;
using Microsoft.Extensions.DependencyInjection;

namespace ETL.Opiniones.Application.Configuration
{
    public static class ApplicationServiceRegistration
    {
        public static IServiceCollection AddApplicationServices(this IServiceCollection services)
        {
            // Orquestador del proceso de extracción
            services.AddTransient<IExtractionOrchestrator, ExtractionOrchestrator>();

            return services;
        }
    }
}
