using Microsoft.AspNetCore.Mvc;

namespace Baby_NI_Loader_BE.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class LoaderController : ControllerBase
    {

        private readonly ILogger<LoaderController> _logger;
        private readonly LoaderService _loaderService;

        public LoaderController(ILogger<LoaderController> logger, LoaderService loaderService)
        {
            _logger = logger;
            _loaderService = loaderService;
        }

        [HttpGet(Name = "GetDataReady")]
        public IActionResult GetDataReady()
        {
            bool isDataReady = _loaderService.GetReadyData();

            return Ok(isDataReady);
        }

    }
}