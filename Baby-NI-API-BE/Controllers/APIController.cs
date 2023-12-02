using Baby_NI_API_BE.Model;
using Microsoft.AspNetCore.Mvc;

namespace Baby_NI_API_BE.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class APIController : ControllerBase
    {
        
        private readonly ILogger<APIController> _logger;
        private readonly DataRepository _dataRepository;

        public APIController(ILogger<APIController> logger, DataRepository dataRepository)
        {
            _logger = logger;
            _dataRepository = dataRepository;
        }

        [HttpGet(Name = "GetDataResult")]
        public IActionResult DataResult()
        {

            Data result = _dataRepository.LoadDataFromDatabase();

            return Ok(result);
        }
    }
}