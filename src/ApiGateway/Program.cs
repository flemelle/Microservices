using Ocelot.DependencyInjection;
using Ocelot.Middleware;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddJsonFile("ocelot.json", optional: false, reloadOnChange: true);
builder.Services.AddOcelot(builder.Configuration);

var app = builder.Build();

// Ocelot est terminal : il prend en charge tout le routage a partir de la configuration ocelot.json
// (voir architecture.md §9 - role de l'API Gateway).
await app.UseOcelot();

app.Run();
