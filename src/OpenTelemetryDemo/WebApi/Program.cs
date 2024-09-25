using Cassandra;
using Cassandra.Mapping;
using Cassandra.OpenTelemetry;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;
using Serilog.Extensions.Logging;
using WebApi.Controllers;
using ISession = Cassandra.ISession;

namespace WebApi
{
    // This API creates a keyspace "weather" and table "weather_forecast" on startup (if they don't exist).
    public class Program
    {
        //private const string CassandraContactPoint = "127.0.0.1";
        //private const int CassandraPort = 9042;

        public static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .MinimumLevel.Information()
                .CreateLogger();

            var builder = WebApplication.CreateBuilder(args);
            builder.Configuration.AddJsonFile("appsettings-local.json");

            // Add services to the container.

            builder.Services.AddOpenTelemetry()
                .ConfigureResource(resource => resource.AddService("Weather Forecast Web API"))
                .WithTracing(tracing => tracing
                    .AddAspNetCoreInstrumentation()
                    .AddSource(CassandraActivitySourceHelper.ActivitySourceName)
                    .AddSource(WeatherForecastController.ActivitySource.Name)
                    .AddOtlpExporter(opt => opt.Endpoint = new Uri(builder.Configuration["OpenTelemetry:ExporterUri"]!)));
            builder.Services.AddLogging(b => b.AddSerilog());
            builder.Services.AddSingleton<ICluster>(services =>
            {
                var l = services.GetService<ILoggerProvider>();
                Cassandra.Diagnostics.AddLoggerProvider(l);
                var cassandraBuilder = Cluster.Builder()
                    //.AddContactPoint(CassandraContactPoint)
                    //.WithPort(CassandraPort)
                    .WithCloudSecureConnectionBundle(builder.Configuration["Astra:Bundle"])
                    .WithCredentials("token", builder.Configuration["Astra:Token"])
                    .WithOpenTelemetryInstrumentation(opts => opts.IncludeDatabaseStatement = true)
                    .WithExecutionProfiles(opt => opt
                        .WithProfile("default", profile => profile
                            .WithRetryPolicy(new IdempotenceAwareRetryPolicy(new LoggingRetryPolicy(new DefaultRetryPolicy())))
                            .WithSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(50, 1))
                            .WithReadTimeoutMillis(10000))
                        .WithProfile("short", profile => profile
                            .WithReadTimeoutMillis(50).WithSpeculativeExecutionPolicy(NoSpeculativeExecutionPolicy.Instance)))
                    .WithQueryOptions(new QueryOptions().SetDefaultIdempotence(true));
                return cassandraBuilder.Build();
            });
            builder.Services.AddSingleton<ISession>(provider =>
            {
                var cluster = provider.GetService<ICluster>();
                if (cluster == null)
                {
                    throw new ArgumentNullException(nameof(cluster));
                }
                return cluster.Connect();
            });
            builder.Services.AddSingleton<IMapper>(provider =>
            {
                var session = provider.GetService<ISession>();
                if (session == null)
                {
                    throw new ArgumentNullException(nameof(session));
                }

                return new Mapper(session);
            });
            builder.Services.AddControllers();
            // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();

            var app = builder.Build();

            // force initialization of C* Session because if it fails then it can not be reused and the app should restart
            // (or the Session should be created before registering it on the service collection with some kind of retries if needed)
            var session = app.Services.GetService<ISession>();
            if (session == null)
            {
                throw new ArgumentNullException(nameof(session));
            }
            var mapper = app.Services.GetService<IMapper>();
            if (mapper == null)
            {
                throw new ArgumentNullException(nameof(mapper));
            }

            await SetupWeatherForecastDb(session, mapper).ConfigureAwait(false);

            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseAuthorization();

            app.MapControllers();

            await app.RunAsync().ConfigureAwait(false);
        }

        private static async Task SetupWeatherForecastDb(ISession session, IMapper mapper)
        {
            //await session.ExecuteAsync(
            //    new SimpleStatement(
            //        "CREATE KEYSPACE IF NOT EXISTS opentelemetry_demo WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"))
            //    .ConfigureAwait(false);

            await session.ExecuteAsync(
                    new SimpleStatement(
                        "CREATE TABLE IF NOT EXISTS opentelemetry_demo.weather_forecast ( id uuid PRIMARY KEY, date timestamp, summary text, temp_c int )"))
                .ConfigureAwait(false);

            var weatherForecasts = new WeatherForecast[]
            {
                new()
                {
                    Date = new DateTime(2024, 9, 18),
                    Id = Guid.Parse("9c9fdc2c-cf59-4ebe-93ac-c26e2fd1a56a"),
                    Summary = "Generally clear. Areas of smoke and haze are possible, reducing visibility at times. High 30\u00b0C. Winds NE at 10 to 15 km/h.",
                    TemperatureC = 30
                },
                new()
                {
                    Date = new DateTime(2024, 9, 19),
                    Id = Guid.Parse("b38b338f-56a8-4f56-a8f1-640d037ed8f6"),
                    Summary = "Generally clear. Areas of smoke and haze are possible, reducing visibility at times. High 28\u00b0C. Winds SSW at 10 to 15 km/h.",
                    TemperatureC = 28
                },
                new()
                {
                    Date = new DateTime(2024, 9, 20),
                    Id = Guid.Parse("04b8e06a-7f59-4921-888f-1a71a52ff7bb"),
                    Summary = "Partly cloudy. High 24\u00b0C. Winds SW at 10 to 15 km/h.",
                    TemperatureC = 24
                },
                new()
                {
                    Date = new DateTime(2024, 9, 21),
                    Id = Guid.Parse("036c25a6-e354-4613-8c27-1822ffb9e184"),
                    Summary = "Rain. High 23\u00b0C. Winds SSW and variable. Chance of rain 70%.",
                    TemperatureC = 23
                },
                new()
                {
                    Date = new DateTime(2024, 9, 22),
                    Id = Guid.Parse("ebd16ca8-ee00-42c1-9763-bb19dbf9a8e9"),
                    Summary = "Morning showers. High 22\u00b0C. Winds SW and variable. Chance of rain 50%.",
                    TemperatureC = 22
                },
            };

            var tasks = weatherForecasts.Select(w => mapper.InsertAsync(w));
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
    }
}
