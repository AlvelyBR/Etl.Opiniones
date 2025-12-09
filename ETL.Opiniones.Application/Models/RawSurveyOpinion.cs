

    namespace ETL.Opiniones.Application.Models
    {
        public class RawSurveyOpinion
        {
            public string IdOpinion { get; set; } = string.Empty;
            public string IdCliente { get; set; } = string.Empty;
            public string IdProducto { get; set; } = string.Empty;
            public DateTime Fecha { get; set; }
            public string Comentario { get; set; } = string.Empty;
            public string Clasificacion { get; set; } = string.Empty;
            public decimal PuntajeSatisfaccion { get; set; }
            public string Fuente { get; set; } = string.Empty;
        }
    }
