using System.Diagnostics;
using Cassandra.Mapping;
using Microsoft.AspNetCore.Mvc;
using OpenTelemetry.Trace;

namespace WebApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController(ILogger<WeatherForecastController> logger, IMapper mapper, TracerProvider tracer) : ControllerBase
    {
        private static Random _r = new Random();
        private static ActivitySource _default = new ActivitySource($"{nameof(WeatherForecastController)} - Default Profile");
        private static ActivitySource _short = new ActivitySource($"{nameof(WeatherForecastController)} - Short Profile");

        [HttpGet(Name = "GetWeatherForecast")]
        public async Task<IEnumerable<WeatherForecast>> GetAsync()
        {
            if (_r.Next() % 5 == 0)
            {
                using var activity = _short.StartActivity();
                var results = await mapper.FetchAsync<WeatherForecast>(Cql.New().WithExecutionProfile("short")).ConfigureAwait(false);
                return results.ToArray();
            }
            else
            {
                using var activity = _default.StartActivity();
                var results = await mapper.FetchAsync<WeatherForecast>().ConfigureAwait(false);
                return results.ToArray();
            }
        }
    }
}
