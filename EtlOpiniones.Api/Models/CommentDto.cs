namespace EtlOpiniones.Api.Models
{
    public class CommentDto
    {
        public string Id { get; set; } = string.Empty;
        public string IdCliente { get; set; } = string.Empty;
        public string IdProducto { get; set; } = string.Empty;
        public DateTime Fecha { get; set; }
        public string Comentario { get; set; } = string.Empty;
        public decimal? Puntuacion { get; set; }
    }
}
