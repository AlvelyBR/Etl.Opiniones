

namespace ETL.Opiniones.Domain.Entities
{
    public class FactOpinion
    {
        public int? TimeKey { get; set; }
        public long? ProductoKey { get; set; }
        public long? ClienteKey { get; set; }
        public long? FuenteKey { get; set; }
        public long? RedSocialKey { get; set; }
        public long? ClasificacionKey { get; set; }

        public string Origen { get; set; } = string.Empty;      // web_reviews, surveys, social, etc.
        public string IdOrigen { get; set; } = string.Empty;    // IdReview, IdOpinion, IdComment, etc.

        public decimal? Puntuacion { get; set; }
        public string? Comentario { get; set; }
        public bool TieneTexto { get; set; }
        public DateTime? FechaOpinion { get; set; }
    }
}
