

namespace ETL.Opiniones.Domain.Entities
{
    public class DimFuenteDatos
    {
        public long FuenteKey { get; set; }
        public string IdFuente { get; set; } = string.Empty;
        public string TipoFuente { get; set; } = string.Empty;
        public DateTime? FechaCarga { get; set; }
    }
}
