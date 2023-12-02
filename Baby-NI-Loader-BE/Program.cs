using Baby_NI_Loader_BE;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// Add configuration if necessary (e.g., appsettings.json)
builder.Configuration.AddJsonFile("appsettings.json");

// Add services to the container.
builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

string loaderPath = builder.Configuration["Configurations:LoaderPath"];
string loggerPath = builder.Configuration["Configurations:LoggerPath"];

// Connection String
string verticaConnection = builder.Configuration["ConnectionStrings:VerticaConnection"];

// Initiate Logging
Log.Logger = new LoggerConfiguration()
    .WriteTo.File(
        loggerPath,
        rollingInterval: RollingInterval.Day,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}"
    )
    .CreateLogger();

// Set up LoggerFactory with Serilog
LoggerFactory loggerFactory = (LoggerFactory)new LoggerFactory().AddSerilog();

// Create the logger
ILogger<Program> logger = loggerFactory.CreateLogger<Program>();


builder.Services.AddSingleton<LoaderService>(provider =>
{
    return new LoaderService(logger, loaderPath, verticaConnection);
});

// Configure CORS services within ConfigureServices
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(builder =>
    {
        builder.AllowAnyOrigin()
               .AllowAnyMethod()
               .AllowAnyHeader();
    });
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// Enable CORS within Configure
app.UseCors();

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

var watcherService = app.Services.GetRequiredService<LoaderService>();

app.Run();
