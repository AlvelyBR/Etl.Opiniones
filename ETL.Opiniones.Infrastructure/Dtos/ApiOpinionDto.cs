
namespace ETL.Opiniones.Infrastructure.Dtos
{
    internal class ApiOpinionDto
    {
        public string? Id { get; set; }
        public string? IdOpinion { get; set; }
        public string IdCliente { get; set; } = string.Empty;
        public string IdProducto { get; set; } = string.Empty;
        public DateTime Fecha { get; set; }
        public string? Comentario { get; set; }
        public decimal? Puntuacion { get; set; }
    }
}
