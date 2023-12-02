using Microsoft.AspNetCore.Mvc;

namespace Baby_NI_Watcher_BE.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WatcherController : ControllerBase
    {
        private readonly ILogger<WatcherController> _logger;
        private readonly WatcherService _watcherService;

        public WatcherController(ILogger<WatcherController> logger, WatcherService watcherService)
        {
            _logger = logger;
            _watcherService = watcherService;
        }

        [HttpGet(Name = "GetFilePath")]
        public IActionResult GetFilePath()
        {
            string result = _watcherService.GetFilePath();
            return Ok(result);
        }
    }
}