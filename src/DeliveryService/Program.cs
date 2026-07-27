using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));

var kafkaBootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "kafka:9092";

var couriers = new ConcurrentDictionary<Guid, Courier>();
var deliveries = new ConcurrentDictionary<Guid, Delivery>();
var deliveriesByOrder = new ConcurrentDictionary<Guid, Guid>();
var chaos = new DeliveryChaosState();

SeedCouriers(couriers);

builder.Services.AddSingleton(couriers);
builder.Services.AddSingleton(deliveries);
builder.Services.AddSingleton(deliveriesByOrder);
builder.Services.AddSingleton(chaos);
builder.Services.AddSingleton(new DeliveryEventPublisher(kafkaBootstrap));
builder.Services.AddHostedService<DeliveryCommandsConsumer>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/v1/delivery/couriers", (CourierCreateRequest req) =>
{
    var courier = new Courier { Id = Guid.NewGuid(), Name = req.Name, Status = CourierStatus.OFFLINE, Location = new GeoPoint(48.8566, 2.3522) };
    couriers[courier.Id] = courier;
    return Results.Created($"/v1/delivery/couriers/{courier.Id}", courier);
});

app.MapPut("/v1/delivery/couriers/{id:guid}/availability", (Guid id, AvailabilityRequest req) =>
{
    if (!couriers.TryGetValue(id, out var courier)) return Results.NotFound(Error("NOT_FOUND", "Livreur introuvable"));
    courier.Status = req.Status;
    if (req.Location is not null) courier.Location = req.Location;
    return Results.Ok(courier);
});

app.MapGet("/v1/delivery/deliveries/{id:guid}", (Guid id) =>
    deliveries.TryGetValue(id, out var d) ? Results.Ok(d) : Results.NotFound(Error("NOT_FOUND", "Livraison introuvable")));

app.MapGet("/v1/delivery/deliveries/order/{orderId:guid}", (Guid orderId) =>
    deliveriesByOrder.TryGetValue(orderId, out var did) && deliveries.TryGetValue(did, out var d)
        ? Results.Ok(d)
        : Results.NotFound(Error("NOT_FOUND", "Livraison introuvable pour cette commande")));

var publisher = app.Services.GetRequiredService<DeliveryEventPublisher>();

app.MapPost("/v1/delivery/deliveries/{id:guid}/confirm", async (Guid id) =>
{
    if (!deliveries.TryGetValue(id, out var delivery)) return Results.NotFound(Error("NOT_FOUND", "Livraison introuvable"));
    delivery.Status = DeliveryStatus.DELIVERED;
    if (delivery.CourierId.HasValue && couriers.TryGetValue(delivery.CourierId.Value, out var courier))
        courier.Status = CourierStatus.AVAILABLE;
    await publisher.PublishAsync("DeliveryCompleted", delivery.OrderId.ToString(), new { orderId = delivery.OrderId, deliveryId = delivery.Id });
    return Results.Accepted();
});

// Pilotage du mode chaos pour la demo (forcer "aucun livreur disponible" -> declenche la compensation SAGA).
app.MapPost("/v1/_chaos/force-no-courier", () => { chaos.ForceNoCourierAvailable = true; return Results.Ok(new { chaos.ForceNoCourierAvailable }); });
app.MapPost("/v1/_chaos/reset", () => { chaos.ForceNoCourierAvailable = false; return Results.Ok(new { chaos.ForceNoCourierAvailable }); });

app.MapGet("/", () => Results.Ok(new { service = "delivery-service", status = "up", couriers = couriers.Count }));

app.Run();

static object Error(string code, string message) => new { error = new { code, message } };

static void SeedCouriers(ConcurrentDictionary<Guid, Courier> couriers)
{
    var seed = new[]
    {
        new Courier { Id = Guid.NewGuid(), Name = "Karim B.", Status = CourierStatus.AVAILABLE, Location = new GeoPoint(48.8566, 2.3522) },
        new Courier { Id = Guid.NewGuid(), Name = "Lucie M.", Status = CourierStatus.AVAILABLE, Location = new GeoPoint(48.8606, 2.3376) },
        new Courier { Id = Guid.NewGuid(), Name = "Yanis T.", Status = CourierStatus.OFFLINE, Location = new GeoPoint(48.8738, 2.2950) },
    };
    foreach (var c in seed) couriers[c.Id] = c;
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

record GeoPoint(double Lat, double Lng);
enum CourierStatus { OFFLINE, AVAILABLE, BUSY }
enum DeliveryStatus { PENDING, ASSIGNED, PICKED_UP, IN_TRANSIT, DELIVERED, FAILED }

class Courier
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public CourierStatus Status { get; set; }
    public GeoPoint Location { get; set; } = new(0, 0);
}

class Delivery
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public Guid? CourierId { get; set; }
    public DeliveryStatus Status { get; set; }
    public GeoPoint? CurrentLocation { get; set; }
}

record CourierCreateRequest(string Name);
record AvailabilityRequest(CourierStatus Status, GeoPoint? Location);

class DeliveryChaosState
{
    public bool ForceNoCourierAvailable { get; set; }
}

/// <summary>Publie sur le topic Kafka "delivery.events".</summary>
class DeliveryEventPublisher : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private const string Topic = "delivery.events";

    public DeliveryEventPublisher(string bootstrapServers)
    {
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            MessageTimeoutMs = 5000
        }).Build();
    }

    public async Task PublishAsync<T>(string eventType, string key, T data)
    {
        var envelope = new { eventType, eventVersion = "v1", occurredAt = DateTime.UtcNow, data };
        try
        {
            await _producer.ProduceAsync(Topic, new Message<string, string> { Key = key, Value = JsonSerializer.Serialize(envelope) });
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[delivery-service] Echec de publication Kafka ({eventType}): {ex.Message}");
        }
    }

    public void Dispose() => _producer.Flush(TimeSpan.FromSeconds(5));
}

record AssignCourierCommand(Guid OrderId);

/// <summary>
/// Consomme "delivery.commands" (AssignCourier, consumer group "delivery-service-cg"). Etape de la SAGA
/// orchestree par order-service. Simule ensuite une progression de statut (PICKED_UP -> IN_TRANSIT)
/// pour illustrer le suivi en temps reel ; la confirmation finale (DELIVERED) reste un acte explicite
/// via POST /v1/delivery/deliveries/{id}/confirm, conformement a l'exigence fonctionnelle.
/// </summary>
class DeliveryCommandsConsumer : BackgroundService
{
    private readonly ConcurrentDictionary<Guid, Courier> _couriers;
    private readonly ConcurrentDictionary<Guid, Delivery> _deliveries;
    private readonly ConcurrentDictionary<Guid, Guid> _deliveriesByOrder;
    private readonly DeliveryChaosState _chaos;
    private readonly DeliveryEventPublisher _publisher;
    private readonly ILogger<DeliveryCommandsConsumer> _logger;
    private readonly string _bootstrapServers;
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public DeliveryCommandsConsumer(ConcurrentDictionary<Guid, Courier> couriers, ConcurrentDictionary<Guid, Delivery> deliveries,
        ConcurrentDictionary<Guid, Guid> deliveriesByOrder, DeliveryChaosState chaos, DeliveryEventPublisher publisher,
        ILogger<DeliveryCommandsConsumer> logger)
    {
        _couriers = couriers;
        _deliveries = deliveries;
        _deliveriesByOrder = deliveriesByOrder;
        _chaos = chaos;
        _publisher = publisher;
        _logger = logger;
        _bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "kafka:9092";
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => Consume(stoppingToken), stoppingToken);

    private void Consume(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "delivery-service-cg",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        IConsumer<string, string>? consumer = null;
        while (!stoppingToken.IsCancellationRequested && consumer is null)
        {
            try
            {
                consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe("delivery.commands");
                _logger.LogInformation("Abonne au topic delivery.commands (groupe delivery-service-cg)");
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
                Handle(result.Message.Value).GetAwaiter().GetResult();
            }
            catch (OperationCanceledException) { break; }
            catch (ConsumeException ex) { _logger.LogError(ex, "Erreur de consommation Kafka"); }
        }
        consumer.Close();
    }

    private async Task Handle(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var eventType = root.GetProperty("eventType").GetString();
            if (eventType != "AssignCourier") return;

            var cmd = root.GetProperty("data").Deserialize<AssignCourierCommand>(JsonOptions)!;

            // Idempotence : ne pas reassigner une commande deja traitee (at-least-once, cf. ADR-0002).
            if (_deliveriesByOrder.ContainsKey(cmd.OrderId))
            {
                _logger.LogInformation("AssignCourier ignore (deja traite) pour la commande {OrderId}", cmd.OrderId);
                return;
            }

            var available = !_chaos.ForceNoCourierAvailable
                ? _couriers.Values.FirstOrDefault(c => c.Status == CourierStatus.AVAILABLE)
                : null;

            if (_chaos.ForceNoCourierAvailable) _chaos.ForceNoCourierAvailable = false; // effet a usage unique pour la demo

            if (available is null)
            {
                await _publisher.PublishAsync("NoCourierAvailable", cmd.OrderId.ToString(),
                    new { orderId = cmd.OrderId, reason = "Aucun livreur disponible dans la zone" });
                return;
            }

            available.Status = CourierStatus.BUSY;
            var delivery = new Delivery { Id = Guid.NewGuid(), OrderId = cmd.OrderId, CourierId = available.Id, Status = DeliveryStatus.ASSIGNED };
            _deliveries[delivery.Id] = delivery;
            _deliveriesByOrder[cmd.OrderId] = delivery.Id;

            await _publisher.PublishAsync("CourierAssigned", cmd.OrderId.ToString(),
                new { orderId = cmd.OrderId, deliveryId = delivery.Id, courierId = available.Id, courierName = available.Name });

            SimulateProgressAsync(delivery);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Impossible de traiter une commande delivery.commands");
        }
    }

    private void SimulateProgressAsync(Delivery delivery)
    {
        _ = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromSeconds(5));
            delivery.Status = DeliveryStatus.PICKED_UP;
            await _publisher.PublishAsync("DeliveryStatusChanged", delivery.OrderId.ToString(),
                new { orderId = delivery.OrderId, deliveryId = delivery.Id, status = "PICKED_UP" });

            await Task.Delay(TimeSpan.FromSeconds(5));
            delivery.Status = DeliveryStatus.IN_TRANSIT;
            await _publisher.PublishAsync("DeliveryStatusChanged", delivery.OrderId.ToString(),
                new { orderId = delivery.OrderId, deliveryId = delivery.Id, status = "IN_TRANSIT" });
        });
    }
}
