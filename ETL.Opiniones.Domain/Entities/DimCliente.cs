

namespace ETL.Opiniones.Domain.Entities
{
    public class DimCliente
    {
        public long ClienteKey { get; set; }
        public string NkCliente { get; set; } = string.Empty; // IdCliente origen
        public string? Nombre { get; set; }
        public string? Email { get; set; }
        public string? Pais { get; set; }
        public string? Ciudad { get; set; }
    }
}
