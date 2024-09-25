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
        private static readonly Random _r = new Random();
        public static ActivitySource ActivitySource = new ActivitySource($"{nameof(WeatherForecastController)}");
        private static long _actionCounter;

        [HttpGet(Name = "GetWeatherForecast")]
        public async Task<IEnumerable<WeatherForecast>> GetAsync()
        {
            var list = new List<WeatherForecast>();
            var action = Interlocked.Increment(ref _actionCounter);
            switch (action % 3)
            {
                case 0:
                    using (ActivitySource.StartActivity("Short Profile"))
                    {
                        var page = await mapper.FetchPageAsync<WeatherForecast>(Cql.New().WithExecutionProfile("short"))
                            .ConfigureAwait(false);
                        list.AddRange(page);
                        while (page.PagingState != null)
                        {
                            page = await mapper.FetchPageAsync<WeatherForecast>(
                                Cql.New()
                                    .WithExecutionProfile("short")
                                    .WithOptions(opt => opt.SetPagingState(page.PagingState))).ConfigureAwait(false);
                            list.AddRange(page);
                        }

                        return list;
                    }

                case 1:
                    using (ActivitySource.StartActivity("Default Profile"))
                    {
                        var page = await mapper.FetchPageAsync<WeatherForecast>(Cql.New()).ConfigureAwait(false);
                        list.AddRange(page);
                        while (page.PagingState != null)
                        {
                            page = await mapper.FetchPageAsync<WeatherForecast>(
                                Cql.New()
                                    .WithOptions(opt => opt.SetPagingState(page.PagingState))).ConfigureAwait(false);
                            list.AddRange(page);
                        }

                        return list;
                    }

                case 2:
                    using (ActivitySource.StartActivity("Default Profile Small Page Size"))
                    {
                        var page = await mapper
                            .FetchPageAsync<WeatherForecast>(Cql.New().WithOptions(opt => opt.SetPageSize(2)))
                            .ConfigureAwait(false);
                        list.AddRange(page);
                        while (page.PagingState != null)
                        {
                            page = await mapper.FetchPageAsync<WeatherForecast>(
                                    Cql.New()
                                        .WithOptions(opt => opt.SetPagingState(page.PagingState).SetPageSize(2)))
                                .ConfigureAwait(false);
                            list.AddRange(page);
                        }

                        return list;
                    }

                default: throw new Exception();
            }
        }
    }
}
