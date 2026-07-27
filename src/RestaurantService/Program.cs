using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var kafkaBootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "kafka:9092";
builder.Services.AddSingleton(new RestaurantEventPublisher(kafkaBootstrap));

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

// ---------------------------------------------------------------------------
// Stockage en memoire (chaque service possede son propre stockage, cf. ADR-0007)
// ---------------------------------------------------------------------------
var restaurants = new ConcurrentDictionary<Guid, Restaurant>();
var menuItems = new ConcurrentDictionary<Guid, MenuItem>();

// Bascule de "chaos" pour demontrer le Circuit Breaker cote order-service (voir README, scenario de demo).
var chaosMode = new ChaosState();

SeedData(restaurants, menuItems);

var publisher = app.Services.GetRequiredService<RestaurantEventPublisher>();
foreach (var r in restaurants.Values)
{
    await publisher.PublishAsync("RestaurantCreated", r.Id.ToString(), r);
    foreach (var mi in menuItems.Values.Where(m => m.RestaurantId == r.Id))
    {
        await publisher.PublishAsync("MenuItemUpserted", r.Id.ToString(), mi);
    }
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

app.MapGet("/v1/restaurants", (int page = 1, int pageSize = 20) =>
{
    var items = restaurants.Values.OrderBy(r => r.Name)
        .Skip((page - 1) * pageSize).Take(pageSize).ToList();
    return Results.Ok(new { items, page, pageSize, total = restaurants.Count });
});

app.MapPost("/v1/restaurants", async (RestaurantCreateRequest req) =>
{
    var restaurant = new Restaurant
    {
        Id = Guid.NewGuid(),
        Name = req.Name,
        CuisineType = req.CuisineType,
        Address = req.Address,
        Location = req.Location ?? new GeoPoint(0, 0),
        IsOpen = true,
        OpeningHours = new List<OpeningHours>()
    };
    restaurants[restaurant.Id] = restaurant;
    await publisher.PublishAsync("RestaurantCreated", restaurant.Id.ToString(), restaurant);
    return Results.Created($"/v1/restaurants/{restaurant.Id}", restaurant);
});

app.MapGet("/v1/restaurants/{id:guid}", (Guid id) =>
    restaurants.TryGetValue(id, out var r) ? Results.Ok(r) : Results.NotFound(Error("NOT_FOUND", "Restaurant introuvable")));

app.MapGet("/v1/restaurants/{id:guid}/menu", (Guid id) =>
{
    if (!restaurants.ContainsKey(id)) return Results.NotFound(Error("NOT_FOUND", "Restaurant introuvable"));
    var items = menuItems.Values.Where(m => m.RestaurantId == id).ToList();
    return Results.Ok(items);
});

app.MapPost("/v1/restaurants/{id:guid}/menu", async (Guid id, MenuItemCreateRequest req) =>
{
    if (!restaurants.ContainsKey(id)) return Results.NotFound(Error("NOT_FOUND", "Restaurant introuvable"));
    var item = new MenuItem
    {
        Id = Guid.NewGuid(),
        RestaurantId = id,
        Name = req.Name,
        Description = req.Description ?? "",
        Price = req.Price,
        Options = req.Options ?? new List<string>(),
        Available = true
    };
    menuItems[item.Id] = item;
    await publisher.PublishAsync("MenuItemUpserted", id.ToString(), item);
    return Results.Created($"/v1/restaurants/{id}/menu/{item.Id}", item);
});

// Endpoint utilise en synchrone par order-service (proteger par Circuit Breaker/Retry/Timeout cote appelant).
app.MapGet("/v1/restaurants/{id:guid}/validate", async (Guid id, string items) =>
{
    if (chaosMode.Enabled)
    {
        // Simule une panne du service pour demontrer le pattern de resilience cote order-service.
        await Task.Delay(chaosMode.DelayMs);
        return Results.Json(Error("CHAOS_MODE", "Panne simulee (mode chaos actif)"), statusCode: StatusCodes.Status500InternalServerError);
    }

    if (!restaurants.TryGetValue(id, out var restaurant))
        return Results.NotFound(Error("NOT_FOUND", "Restaurant introuvable"));

    var requestedIds = items.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(Guid.Parse).ToList();

    var validated = requestedIds.Select(itemId =>
    {
        menuItems.TryGetValue(itemId, out var mi);
        return new
        {
            menuItemId = itemId,
            name = mi?.Name ?? "unknown",
            price = mi?.Price ?? 0,
            available = mi is { Available: true }
        };
    }).ToList();

    var allAvailable = validated.All(v => v.available);
    var result = new { restaurantOpen = restaurant.IsOpen, allItemsAvailable = allAvailable, items = validated };

    return restaurant.IsOpen && allAvailable
        ? Results.Ok(result)
        : Results.Json(result, statusCode: StatusCodes.Status409Conflict);
});

app.MapPost("/v1/restaurants/{id:guid}/orders/{orderId:guid}/accept", async (Guid id, Guid orderId) =>
{
    if (!restaurants.ContainsKey(id)) return Results.NotFound(Error("NOT_FOUND", "Restaurant introuvable"));
    await publisher.PublishAsync("OrderAccepted", orderId.ToString(), new { orderId, restaurantId = id });
    return Results.Accepted();
});

app.MapPost("/v1/restaurants/{id:guid}/orders/{orderId:guid}/reject", async (Guid id, Guid orderId, RejectRequest? req) =>
{
    if (!restaurants.ContainsKey(id)) return Results.NotFound(Error("NOT_FOUND", "Restaurant introuvable"));
    await publisher.PublishAsync("OrderRejected", orderId.ToString(), new { orderId, restaurantId = id, reason = req?.Reason });
    return Results.Accepted();
});

// Endpoints de demo (hors contrat OpenAPI officiel) pour piloter le mode chaos pendant la soutenance.
app.MapPost("/v1/_chaos/enable", (int delayMs = 3000) => { chaosMode.Enable(delayMs); return Results.Ok(new { chaosMode.Enabled, delayMs }); });
app.MapPost("/v1/_chaos/disable", () => { chaosMode.Disable(); return Results.Ok(new { chaosMode.Enabled }); });

app.MapGet("/", () => Results.Ok(new { service = "restaurant-service", status = "up" }));

app.Run();

static object Error(string code, string message) => new { error = new { code, message } };

static void SeedData(ConcurrentDictionary<Guid, Restaurant> restaurants, ConcurrentDictionary<Guid, MenuItem> menuItems)
{
    var pizzeria = new Restaurant
    {
        Id = Guid.Parse("11111111-1111-1111-1111-111111111111"),
        Name = "Pizzeria Bella Napoli",
        CuisineType = "Italien",
        Address = "12 rue de Rome, Paris",
        Location = new GeoPoint(48.8566, 2.3522),
        IsOpen = true,
        OpeningHours = new List<OpeningHours> { new("MONDAY", "11:30", "22:00") }
    };
    var sushi = new Restaurant
    {
        Id = Guid.Parse("22222222-2222-2222-2222-222222222222"),
        Name = "Sushi Sakura",
        CuisineType = "Japonais",
        Address = "5 avenue du Japon, Paris",
        Location = new GeoPoint(48.8606, 2.3376),
        IsOpen = true,
        OpeningHours = new List<OpeningHours> { new("MONDAY", "11:30", "22:30") }
    };

    restaurants[pizzeria.Id] = pizzeria;
    restaurants[sushi.Id] = sushi;

    var items = new[]
    {
        new MenuItem { Id = Guid.Parse("aaaaaaaa-0000-0000-0000-000000000001"), RestaurantId = pizzeria.Id, Name = "Pizza Margherita", Description = "Tomate, mozzarella, basilic", Price = 11.50m, Available = true, Options = new() { "Extra fromage" } },
        new MenuItem { Id = Guid.Parse("aaaaaaaa-0000-0000-0000-000000000002"), RestaurantId = pizzeria.Id, Name = "Pizza Regina", Description = "Tomate, mozzarella, jambon, champignons", Price = 13.00m, Available = true, Options = new() },
        new MenuItem { Id = Guid.Parse("bbbbbbbb-0000-0000-0000-000000000001"), RestaurantId = sushi.Id, Name = "Plateau California x12", Description = "Saumon, avocat, surimi", Price = 15.90m, Available = true, Options = new() },
        new MenuItem { Id = Guid.Parse("bbbbbbbb-0000-0000-0000-000000000002"), RestaurantId = sushi.Id, Name = "Ramen Tonkotsu", Description = "Bouillon porc, oeuf mariné, nouilles", Price = 12.90m, Available = true, Options = new() },
    };

    foreach (var item in items) menuItems[item.Id] = item;
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

record GeoPoint(double Lat, double Lng);
record OpeningHours(string Day, string OpensAt, string ClosesAt);

class Restaurant
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public string CuisineType { get; set; } = "";
    public string Address { get; set; } = "";
    public GeoPoint Location { get; set; } = new(0, 0);
    public bool IsOpen { get; set; }
    public List<OpeningHours> OpeningHours { get; set; } = new();
}

class MenuItem
{
    public Guid Id { get; set; }
    public Guid RestaurantId { get; set; }
    public string Name { get; set; } = "";
    public string Description { get; set; } = "";
    public decimal Price { get; set; }
    public List<string> Options { get; set; } = new();
    public bool Available { get; set; }
}

record RestaurantCreateRequest(string Name, string CuisineType, string Address, GeoPoint? Location);
record MenuItemCreateRequest(string Name, string? Description, decimal Price, List<string>? Options);
record RejectRequest(string? Reason);

class ChaosState
{
    public bool Enabled { get; private set; }
    public int DelayMs { get; private set; } = 3000;
    public void Enable(int delayMs) { Enabled = true; DelayMs = delayMs; }
    public void Disable() => Enabled = false;
}

/// <summary>
/// Publie les evenements de domaine de restaurant-service sur le topic Kafka "restaurant.events".
/// Consomme par catalog-service pour reconstruire sa projection en lecture (CQRS, voir ADR-0006).
/// </summary>
class RestaurantEventPublisher : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private const string Topic = "restaurant.events";

    public RestaurantEventPublisher(string bootstrapServers)
    {
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            MessageTimeoutMs = 10000
        }).Build();
    }

    public async Task PublishAsync<T>(string eventType, string key, T data)
    {
        var envelope = new { eventType, eventVersion = "v1", occurredAt = DateTime.UtcNow, data };
        var json = JsonSerializer.Serialize(envelope);
        try
        {
            await _producer.ProduceAsync(Topic, new Message<string, string> { Key = key, Value = json });
        }
        catch (Exception ex)
        {
            // Une panne de publication ne doit jamais faire echouer la requete HTTP en cours
            // (la coherence entre restaurant-service et catalog-service reste "eventuelle" par design, cf. ADR-0006).
            Console.Error.WriteLine($"[restaurant-service] Echec de publication Kafka ({eventType}): {ex.Message}");
        }
    }

    public void Dispose() => _producer.Flush(TimeSpan.FromSeconds(5));
}
