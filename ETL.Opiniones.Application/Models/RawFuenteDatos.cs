

namespace ETL.Opiniones.Application.Models
{
    public class RawFuenteDatos
    {
        public string IdFuente { get; set; } = string.Empty;
        public string TipoFuente { get; set; } = string.Empty;
        public DateTime? FechaCarga { get; set; }
    }
}
