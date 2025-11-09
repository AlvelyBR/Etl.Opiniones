

namespace ETL.Opiniones.Domain.Entities
{
    public class DimProducto
    {
        public long ProductoKey { get; set; }
        public int IdProducto { get; set; }
        public string Nombre { get; set; } = string.Empty;
        public string NombreCategoria { get; set; } = string.Empty;
    }
}
