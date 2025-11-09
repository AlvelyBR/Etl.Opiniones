

namespace ETL.Opiniones.Application.Models
{
    public class RawWebReview
    {
        public string IdReview { get; set; } = string.Empty;
        public string IdCliente { get; set; } = string.Empty;
        public string IdProducto { get; set; } = string.Empty;
        public DateTime Fecha { get; set; }
        public string Comentario { get; set; } = string.Empty;
        public decimal Rating { get; set; }
    }
}
