using Microsoft.AspNetCore.Mvc;

namespace Baby_NI_Parser_BE.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ParserController : ControllerBase
    {
        private readonly ILogger<ParserController> _logger;
        private readonly ParserService _parserService;

        public ParserController(ILogger<ParserController> logger, ParserService parserService)
        {
            _logger = logger;
            _parserService = parserService;
        }

        [HttpGet(Name = "GetFilePath")]
        public IActionResult GetFilePath()
        {
            string filePath = _parserService.GetFile();

            return Ok(filePath);
        }
    }
}