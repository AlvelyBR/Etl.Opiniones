

namespace ETL.Opiniones.Domain.Entities
{
    public class DimRedSocial
    {
        public long RedSocialKey { get; set; }
        public string Nombre { get; set; } = string.Empty;
        public long FuenteKey { get; set; }
    }
}
