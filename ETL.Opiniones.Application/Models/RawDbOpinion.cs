

namespace ETL.Opiniones.Application.Models
{
    public class RawDbOpinion
    {
        public int IdOpinion { get; set; }

        public int IdCliente { get; set; }
        public string ClienteNombre { get; set; } = string.Empty;
        public string ClienteEmail { get; set; } = string.Empty;

        public string IdProducto { get; set; } = string.Empty;
        public string ProductoNombre { get; set; } = string.Empty;
        public string? CategoriaNombre { get; set; }

        public DateTime Fecha { get; set; }
        public string? Comentario { get; set; }

        public int? PuntajeSatisfaccion { get; set; }
        public int? Rating { get; set; }

        public int IdFuente { get; set; }
        public string? TipoFuente { get; set; }

        public string TipoOpinion { get; set; } = string.Empty;       // tipo_opinion
        public string? FuenteSocial { get; set; }                      // fuente_social
        public string? ClasificacionNombre { get; set; }               // clasificaciones.nombre
    }
}
