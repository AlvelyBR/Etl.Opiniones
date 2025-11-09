using EtlOpiniones.Api.Models;
using Microsoft.AspNetCore.Mvc;

namespace EtlOpiniones.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CommentsController : ControllerBase
    {
       
        private static readonly List<CommentDto> _comments = new List<CommentDto>
        {
            new CommentDto
            {
                Id = "A001",
                IdCliente = "C001",
                IdProducto = "P001",
                Fecha = DateTime.UtcNow.AddDays(-2),
                Comentario = "Comentario inicial desde la API.",
                Puntuacion = 4.5m
            }
        };

        [HttpGet]
        public ActionResult<IEnumerable<CommentDto>> Get()
        {
            return Ok(_comments);
        }

        [HttpPost]
        public ActionResult<CommentDto> Post([FromBody] CommentDto comment)
        {
            if (string.IsNullOrWhiteSpace(comment.Id))
            {
                comment.Id = Guid.NewGuid().ToString();
            }

            if (comment.Fecha == default)
            {
                comment.Fecha = DateTime.UtcNow;
            }

            _comments.Add(comment);

            return CreatedAtAction(nameof(Get), new { id = comment.Id }, comment);
        }
    }
}
