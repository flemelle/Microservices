using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var store = new ConcurrentDictionary<Guid, RestaurantView>();
builder.Services.AddSingleton(store);
builder.Services.AddHostedService<RestaurantEventsConsumer>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/v1/catalog/restaurants", (double? lat, double? lng, double radiusKm = 5, string? cuisineType = null, int page = 1, int pageSize = 20) =>
{
    IEnumerable<RestaurantView> query = store.Values;

    if (!string.IsNullOrWhiteSpace(cuisineType))
        query = query.Where(r => string.Equals(r.CuisineType, cuisineType, StringComparison.OrdinalIgnoreCase));

    if (lat.HasValue && lng.HasValue)
        query = query.Where(r => Haversine(lat.Value, lng.Value, r.Location.Lat, r.Location.Lng) <= radiusKm);

    var all = query.OrderBy(r => r.Name).ToList();
    var items = all.Skip((page - 1) * pageSize).Take(pageSize).ToList();
    return Results.Ok(new { items, page, pageSize, total = all.Count });
});

app.MapGet("/v1/catalog/restaurants/{id:guid}", (Guid id) =>
    store.TryGetValue(id, out var r) ? Results.Ok(r) : Results.NotFound(Error("NOT_FOUND", "Restaurant introuvable dans la projection (peut etre en cours de synchronisation)")));

app.MapGet("/v1/catalog/search", (string dish, double? lat = null, double? lng = null) =>
{
    var results = store.Values
        .SelectMany(r => r.MenuItems
            .Where(mi => mi.Name.Contains(dish, StringComparison.OrdinalIgnoreCase))
            .Select(mi => new { restaurantId = r.Id, restaurantName = r.Name, menuItem = mi }));
    return Results.Ok(results.ToList());
});

app.MapGet("/", () => Results.Ok(new { service = "catalog-service", status = "up", projectionSize = store.Count }));

app.Run();

static object Error(string code, string message) => new { error = new { code, message } };

static double Haversine(double lat1, double lng1, double lat2, double lng2)
{
    const double R = 6371;
    double dLat = (lat2 - lat1) * Math.PI / 180;
    double dLng = (lng2 - lng1) * Math.PI / 180;
    double a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
               Math.Cos(lat1 * Math.PI / 180) * Math.Cos(lat2 * Math.PI / 180) *
               Math.Sin(dLng / 2) * Math.Sin(dLng / 2);
    double c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
    return R * c;
}

// ---------------------------------------------------------------------------
// Modele de lecture (projection CQRS) - voir architecture.md §10 et ADR-0006
// ---------------------------------------------------------------------------

record GeoPoint(double Lat, double Lng);
record MenuItemView(Guid Id, string Name, decimal Price, bool Available);
record RestaurantView(Guid Id, string Name, string CuisineType, GeoPoint Location, bool IsOpen, List<MenuItemView> MenuItems, DateTime LastSyncedAt);

// DTOs de deserialisation des evenements produits par restaurant-service
record RestaurantDto(Guid Id, string Name, string CuisineType, string Address, GeoPoint Location, bool IsOpen);
record MenuItemDto(Guid Id, Guid RestaurantId, string Name, string Description, decimal Price, List<string> Options, bool Available);
record AvailabilityDto(Guid RestaurantId, bool IsOpen);

/// <summary>
/// Consomme le topic Kafka "restaurant.events" (consumer group "catalog-service-cg") et reconstruit
/// la projection en lecture. Cote lecture du CQRS : aucune ecriture n'est jamais exposee aux clients.
/// </summary>
class RestaurantEventsConsumer : BackgroundService
{
    private readonly ConcurrentDictionary<Guid, RestaurantView> _store;
    private readonly string _bootstrapServers;
    private readonly ILogger<RestaurantEventsConsumer> _logger;
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public RestaurantEventsConsumer(ConcurrentDictionary<Guid, RestaurantView> store, ILogger<RestaurantEventsConsumer> logger)
    {
        _store = store;
        _logger = logger;
        _bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "kafka:9092";
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => Consume(stoppingToken), stoppingToken);

    private void Consume(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "catalog-service-cg",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        IConsumer<string, string>? consumer = null;

        // Boucle de reconnexion : Kafka peut ne pas encore etre pret au demarrage du conteneur.
        while (!stoppingToken.IsCancellationRequested && consumer is null)
        {
            try
            {
                consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe("restaurant.events");
                _logger.LogInformation("Abonne au topic restaurant.events (groupe catalog-service-cg)");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Kafka indisponible, nouvelle tentative dans 5s");
                Thread.Sleep(5000);
            }
        }

        if (consumer is null) return;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                Handle(result.Message.Value);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Erreur de consommation Kafka");
            }
        }

        consumer.Close();
    }

    private void Handle(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var eventType = root.GetProperty("eventType").GetString();
            var dataElement = root.GetProperty("data");

            switch (eventType)
            {
                case "RestaurantCreated":
                {
                    var r = dataElement.Deserialize<RestaurantDto>(JsonOptions)!;
                    _store.AddOrUpdate(r.Id,
                        _ => new RestaurantView(r.Id, r.Name, r.CuisineType, r.Location, r.IsOpen, new List<MenuItemView>(), DateTime.UtcNow),
                        (_, existing) => existing with { Name = r.Name, CuisineType = r.CuisineType, Location = r.Location, IsOpen = r.IsOpen, LastSyncedAt = DateTime.UtcNow });
                    break;
                }
                case "MenuItemUpserted":
                {
                    var mi = dataElement.Deserialize<MenuItemDto>(JsonOptions)!;
                    _store.AddOrUpdate(mi.RestaurantId,
                        _ => new RestaurantView(mi.RestaurantId, "(en cours de synchronisation)", "", new GeoPoint(0, 0), true,
                            new List<MenuItemView> { new(mi.Id, mi.Name, mi.Price, mi.Available) }, DateTime.UtcNow),
                        (_, existing) =>
                        {
                            var items = existing.MenuItems.Where(i => i.Id != mi.Id).ToList();
                            items.Add(new MenuItemView(mi.Id, mi.Name, mi.Price, mi.Available));
                            return existing with { MenuItems = items, LastSyncedAt = DateTime.UtcNow };
                        });
                    break;
                }
                case "RestaurantAvailabilityChanged":
                {
                    var payload = dataElement.Deserialize<AvailabilityDto>(JsonOptions)!;
                    _store.AddOrUpdate(payload.RestaurantId,
                        _ => new RestaurantView(payload.RestaurantId, "(en cours de synchronisation)", "", new GeoPoint(0, 0), payload.IsOpen, new List<MenuItemView>(), DateTime.UtcNow),
                        (_, existing) => existing with { IsOpen = payload.IsOpen, LastSyncedAt = DateTime.UtcNow });
                    break;
                }
                default:
                    // Tolerant reader : un type d'evenement inconnu ou non pertinent pour la projection est ignore.
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Impossible de traiter un evenement restaurant.events");
        }
    }
}
