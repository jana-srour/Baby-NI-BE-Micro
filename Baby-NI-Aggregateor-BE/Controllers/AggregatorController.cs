using Microsoft.AspNetCore.Mvc;

namespace Baby_NI_Aggregateor_BE.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class AggregatorController : ControllerBase
    {

        private readonly ILogger<AggregatorController> _logger;
        private readonly AggregatorService _aggregatorService;

        public AggregatorController(ILogger<AggregatorController> logger, AggregatorService aggregatorService)
        {
            _logger = logger;
            _aggregatorService = aggregatorService;
        }

        [HttpGet(Name = "ReceiveData")]
        public IActionResult ReceiveData()
        {
            bool isDataReady;

            _aggregatorService.StartAggregation();

            return Ok("Data Recieved and aggregation Done");

        }
    }
}