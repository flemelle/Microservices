using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using OrderService.Resilience;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));

// ---------------------------------------------------------------------------
// Client resilient vers restaurant-service : Circuit Breaker(Retry(Timeout(HTTP))) - voir ADR-0004.
// ---------------------------------------------------------------------------
var restaurantServiceUrl = Environment.GetEnvironmentVariable("RESTAURANT_SERVICE_URL") ?? "http://restaurant-service:8080";
builder.Services
    .AddHttpClient<IRestaurantClient, RestaurantClient>(client =>
    {
        client.BaseAddress = new Uri(restaurantServiceUrl);
        client.Timeout = TimeSpan.FromSeconds(30); // garde-fou global ; les policies bornent chaque tentative a 2s
    })
    .AddPolicyHandler(ResiliencePolicies.CircuitBreakerPolicy())
    .AddPolicyHandler(ResiliencePolicies.RetryPolicy())
    .AddPolicyHandler(ResiliencePolicies.TimeoutPolicy());

var kafkaBootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "kafka:9092";

var orders = new ConcurrentDictionary<Guid, Order>();
builder.Services.AddSingleton(orders);
builder.Services.AddSingleton(new OrderEventPublisher(kafkaBootstrap));
builder.Services.AddHostedService<SagaResultsConsumer>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

var publisher = app.Services.GetRequiredService<OrderEventPublisher>();

// ---------------------------------------------------------------------------
// POST /v1/orders - point d'entree de la SAGA "Passage de commande" (cf. architecture.md §7)
// ---------------------------------------------------------------------------
app.MapPost("/v1/orders", async (OrderCreateRequest req, IRestaurantClient restaurantClient, CancellationToken ct) =>
{
    if (req.Items.Count == 0)
        return Results.BadRequest(Error("EMPTY_CART", "La commande doit contenir au moins un article"));

    // Etape 1 (synchrone, resiliente) : valider le restaurant et les prix avant d'engager la SAGA.
    var validation = await restaurantClient.ValidateAsync(req.RestaurantId, req.Items.Select(i => i.MenuItemId), ct);

    if (!validation.CallSucceeded)
        return Results.Json(Error("RESTAURANT_SERVICE_UNAVAILABLE", validation.ErrorMessage ?? "Service restaurant indisponible, merci de reessayer plus tard"),
            statusCode: StatusCodes.Status503ServiceUnavailable);

    if (!validation.RestaurantFound)
        return Results.NotFound(Error("RESTAURANT_NOT_FOUND", "Restaurant introuvable"));

    if (!validation.RestaurantOpen || !validation.AllItemsAvailable)
        return Results.Json(Error("RESTAURANT_NOT_AVAILABLE", "Restaurant ferme ou article indisponible"), statusCode: StatusCodes.Status409Conflict);

    var items = req.Items.Select(reqItem =>
    {
        var validated = validation.Items.First(v => v.MenuItemId == reqItem.MenuItemId);
        return new OrderItem { MenuItemId = reqItem.MenuItemId, Name = validated.Name, UnitPrice = validated.Price, Quantity = reqItem.Quantity };
    }).ToList();

    var subtotal = items.Sum(i => i.UnitPrice * i.Quantity);
    const decimal deliveryFee = 2.90m;

    var order = new Order
    {
        Id = Guid.NewGuid(),
        CustomerId = req.CustomerId,
        RestaurantId = req.RestaurantId,
        Items = items,
        Subtotal = subtotal,
        DeliveryFee = deliveryFee,
        Total = subtotal + deliveryFee,
        Status = OrderStatus.CREATED,
        CreatedAt = DateTime.UtcNow
    };
    order.AddHistory(OrderStatus.CREATED, "Commande creee, en attente de paiement");
    orders[order.Id] = order;

    await publisher.PublishAsync("OrderCreated", order.Id.ToString(), new { orderId = order.Id, customerId = order.CustomerId, restaurantId = order.RestaurantId, total = order.Total });

    // Etape 2 (asynchrone) : declenche le paiement via Kafka - order-service n'attend pas la reponse ici,
    // il reagira a PaymentSucceeded/PaymentFailed (voir SagaResultsConsumer).
    order.Status = OrderStatus.AWAITING_PAYMENT;
    order.AddHistory(OrderStatus.AWAITING_PAYMENT, "Paiement demande");
    await publisher.PublishCommandAsync("payment.commands", "ProcessPayment", order.Id.ToString(), new { orderId = order.Id, amount = order.Total, currency = "EUR" });

    return Results.Created($"/v1/orders/{order.Id}", order);
});

app.MapGet("/v1/orders/{id:guid}", (Guid id) =>
    orders.TryGetValue(id, out var o) ? Results.Ok(o) : Results.NotFound(Error("NOT_FOUND", "Commande introuvable")));

app.MapGet("/v1/orders/{id:guid}/status", (Guid id) =>
    orders.TryGetValue(id, out var o) ? Results.Ok(o.StatusHistory) : Results.NotFound(Error("NOT_FOUND", "Commande introuvable")));

app.MapPost("/v1/orders/{id:guid}/cancel", async (Guid id) =>
{
    if (!orders.TryGetValue(id, out var order)) return Results.NotFound(Error("NOT_FOUND", "Commande introuvable"));

    if (order.Status is OrderStatus.DELIVERED or OrderStatus.CANCELLED)
        return Results.Json(Error("INVALID_STATE", $"Commande dans l'etat {order.Status}, annulation impossible"), statusCode: StatusCodes.Status409Conflict);

    // Compensation : si un paiement a deja ete capture, on le rembourse avant d'annuler (cf. architecture.md §7.4).
    if (order.Status is OrderStatus.PAID or OrderStatus.AWAITING_COURIER or OrderStatus.CONFIRMED or OrderStatus.IN_PREPARATION or OrderStatus.IN_DELIVERY)
    {
        await publisher.PublishCommandAsync("payment.commands", "RefundPayment", order.Id.ToString(), new { orderId = order.Id, amount = (decimal?)null });
        order.AddHistory(order.Status, "Annulation demandee par le client, remboursement en cours");
    }
    else
    {
        order.Status = OrderStatus.CANCELLED;
        order.AddHistory(OrderStatus.CANCELLED, "Annulee par le client avant paiement");
        await publisher.PublishAsync("OrderCancelled", order.Id.ToString(), new { orderId = order.Id });
    }

    return Results.Accepted();
});

app.MapGet("/", () => Results.Ok(new { service = "order-service", status = "up" }));

app.Run();

static object Error(string code, string message) => new { error = new { code, message } };

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

enum OrderStatus { CREATED, AWAITING_PAYMENT, PAID, AWAITING_COURIER, CONFIRMED, IN_PREPARATION, IN_DELIVERY, DELIVERED, CANCELLED }

class OrderItem
{
    public Guid MenuItemId { get; set; }
    public string Name { get; set; } = "";
    public decimal UnitPrice { get; set; }
    public int Quantity { get; set; }
}

record OrderStatusEvent(OrderStatus Status, DateTime OccurredAt, string? Detail);

record DeliveryAddress(string? Street, string? City, double? Lat, double? Lng);

class Order
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
    public Guid RestaurantId { get; set; }
    public List<OrderItem> Items { get; set; } = new();
    public decimal Subtotal { get; set; }
    public decimal DeliveryFee { get; set; }
    public decimal Total { get; set; }
    public OrderStatus Status { get; set; }
    public Guid? CourierId { get; set; }
    public DateTime CreatedAt { get; set; }
    public List<OrderStatusEvent> StatusHistory { get; } = new();

    public void AddHistory(OrderStatus status, string? detail = null) =>
        StatusHistory.Add(new OrderStatusEvent(status, DateTime.UtcNow, detail));
}

record OrderItemRequest(Guid MenuItemId, int Quantity);
record OrderCreateRequest(Guid CustomerId, Guid RestaurantId, List<OrderItemRequest> Items, DeliveryAddress? DeliveryAddress);

/// <summary>Publie les commandes/evenements Kafka produits par order-service (order.events, payment.commands, delivery.commands).</summary>
class OrderEventPublisher : IDisposable
{
    private readonly IProducer<string, string> _producer;

    public OrderEventPublisher(string bootstrapServers)
    {
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            MessageTimeoutMs = 10000
        }).Build();
    }

    public Task PublishAsync<T>(string eventType, string key, T data) => PublishCommandAsync("order.events", eventType, key, data);

    public async Task PublishCommandAsync<T>(string topic, string eventType, string key, T data)
    {
        var envelope = new { eventType, eventVersion = "v1", occurredAt = DateTime.UtcNow, data };
        try
        {
            await _producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = JsonSerializer.Serialize(envelope) });
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[order-service] Echec de publication Kafka ({topic}/{eventType}): {ex.Message}");
        }
    }

    public void Dispose() => _producer.Flush(TimeSpan.FromSeconds(5));
}

// DTOs de deserialisation des evenements consommes (payment.events, delivery.events)
record PaymentSucceededDto(Guid OrderId, Guid PaymentId, decimal Amount, string Currency);
record PaymentFailedDto(Guid OrderId, string Reason);
record PaymentRefundedDto(Guid OrderId, Guid PaymentId, decimal Amount);
record CourierAssignedDto(Guid OrderId, Guid DeliveryId, Guid CourierId, string CourierName);
record NoCourierAvailableDto(Guid OrderId, string Reason);
record DeliveryStatusChangedDto(Guid OrderId, Guid DeliveryId, string Status);
record DeliveryCompletedDto(Guid OrderId, Guid DeliveryId);

/// <summary>
/// Coeur de l'orchestration de la SAGA : consomme "payment.events" et "delivery.events"
/// (consumer group "order-service-cg") et fait progresser (ou compense) la commande en consequence.
/// Voir architecture.md §7 et le diagramme de sequence §12.3.
/// </summary>
class SagaResultsConsumer : BackgroundService
{
    private readonly ConcurrentDictionary<Guid, Order> _orders;
    private readonly OrderEventPublisher _publisher;
    private readonly ILogger<SagaResultsConsumer> _logger;
    private readonly string _bootstrapServers;
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public SagaResultsConsumer(ConcurrentDictionary<Guid, Order> orders, OrderEventPublisher publisher, ILogger<SagaResultsConsumer> logger)
    {
        _orders = orders;
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
            GroupId = "order-service-cg",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        IConsumer<string, string>? consumer = null;
        while (!stoppingToken.IsCancellationRequested && consumer is null)
        {
            try
            {
                consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe(new[] { "payment.events", "delivery.events" });
                _logger.LogInformation("Abonne a payment.events et delivery.events (groupe order-service-cg)");
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
                Handle(result.Topic, result.Message.Value).GetAwaiter().GetResult();
            }
            catch (OperationCanceledException) { break; }
            catch (ConsumeException ex) { _logger.LogError(ex, "Erreur de consommation Kafka"); }
        }
        consumer.Close();
    }

    private async Task Handle(string topic, string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var eventType = root.GetProperty("eventType").GetString();
            var data = root.GetProperty("data");

            switch (eventType)
            {
                case "PaymentSucceeded": await OnPaymentSucceeded(data.Deserialize<PaymentSucceededDto>(JsonOptions)!); break;
                case "PaymentFailed": await OnPaymentFailed(data.Deserialize<PaymentFailedDto>(JsonOptions)!); break;
                case "PaymentRefunded": await OnPaymentRefunded(data.Deserialize<PaymentRefundedDto>(JsonOptions)!); break;
                case "CourierAssigned": await OnCourierAssigned(data.Deserialize<CourierAssignedDto>(JsonOptions)!); break;
                case "NoCourierAvailable": await OnNoCourierAvailable(data.Deserialize<NoCourierAvailableDto>(JsonOptions)!); break;
                case "DeliveryStatusChanged": await OnDeliveryStatusChanged(data.Deserialize<DeliveryStatusChangedDto>(JsonOptions)!); break;
                case "DeliveryCompleted": await OnDeliveryCompleted(data.Deserialize<DeliveryCompletedDto>(JsonOptions)!); break;
                default: break; // tolerant reader
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Impossible de traiter un evenement du topic {Topic}", topic);
        }
    }

    private async Task OnPaymentSucceeded(PaymentSucceededDto evt)
    {
        if (!_orders.TryGetValue(evt.OrderId, out var order) || order.Status != OrderStatus.AWAITING_PAYMENT)
            return; // idempotence : deja traite ou commande inconnue

        order.Status = OrderStatus.PAID;
        order.AddHistory(OrderStatus.PAID, "Paiement capture");
        order.Status = OrderStatus.AWAITING_COURIER;
        order.AddHistory(OrderStatus.AWAITING_COURIER, "Recherche d'un livreur");

        await _publisher.PublishCommandAsync("delivery.commands", "AssignCourier", order.Id.ToString(), new { orderId = order.Id });
    }

    private async Task OnPaymentFailed(PaymentFailedDto evt)
    {
        if (!_orders.TryGetValue(evt.OrderId, out var order) || order.Status != OrderStatus.AWAITING_PAYMENT)
            return;

        order.Status = OrderStatus.CANCELLED;
        order.AddHistory(OrderStatus.CANCELLED, $"Paiement refuse : {evt.Reason}");
        await _publisher.PublishAsync("OrderCancelled", order.Id.ToString(), new { orderId = order.Id, reason = evt.Reason });
    }

    private async Task OnPaymentRefunded(PaymentRefundedDto evt)
    {
        // Point d'arrivee de la compensation (cf. §7.4) : declenchee soit par NoCourierAvailable, soit par une annulation client.
        if (!_orders.TryGetValue(evt.OrderId, out var order) || order.Status == OrderStatus.CANCELLED)
            return;

        order.Status = OrderStatus.CANCELLED;
        order.AddHistory(OrderStatus.CANCELLED, "Commande annulee, paiement rembourse");
        await _publisher.PublishAsync("OrderCancelled", order.Id.ToString(), new { orderId = order.Id, reason = "Remboursement effectue (compensation SAGA)" });
    }

    private async Task OnCourierAssigned(CourierAssignedDto evt)
    {
        if (!_orders.TryGetValue(evt.OrderId, out var order) || order.Status != OrderStatus.AWAITING_COURIER)
            return;

        order.CourierId = evt.CourierId;
        order.Status = OrderStatus.CONFIRMED;
        order.AddHistory(OrderStatus.CONFIRMED, $"Livreur assigne : {evt.CourierName}");
        await _publisher.PublishAsync("OrderConfirmed", order.Id.ToString(), new { orderId = order.Id, courierId = evt.CourierId });

        // Le restaurant commence la preparation des lors que la commande est confirmee (cf. §3.1 restaurant-service).
        order.Status = OrderStatus.IN_PREPARATION;
        order.AddHistory(OrderStatus.IN_PREPARATION, "Restaurant en preparation");
    }

    private async Task OnNoCourierAvailable(NoCourierAvailableDto evt)
    {
        // COMPENSATION : aucun livreur disponible -> on rembourse le paiement deja capture (cf. §7.4).
        if (!_orders.TryGetValue(evt.OrderId, out var order) || order.Status != OrderStatus.AWAITING_COURIER)
            return;

        order.AddHistory(order.Status, $"Aucun livreur disponible ({evt.Reason}), remboursement en cours");
        await _publisher.PublishCommandAsync("payment.commands", "RefundPayment", order.Id.ToString(), new { orderId = order.Id, amount = (decimal?)null });
    }

    private async Task OnDeliveryStatusChanged(DeliveryStatusChangedDto evt)
    {
        if (!_orders.TryGetValue(evt.OrderId, out var order)) return;

        if (evt.Status == "PICKED_UP" && order.Status == OrderStatus.IN_PREPARATION)
        {
            order.Status = OrderStatus.IN_DELIVERY;
            order.AddHistory(OrderStatus.IN_DELIVERY, "Livreur en route");
        }
        else if (evt.Status == "IN_TRANSIT" && order.Status == OrderStatus.IN_DELIVERY)
        {
            order.AddHistory(OrderStatus.IN_DELIVERY, "En transit vers le client");
        }
        await Task.CompletedTask;
    }

    private async Task OnDeliveryCompleted(DeliveryCompletedDto evt)
    {
        if (!_orders.TryGetValue(evt.OrderId, out var order) || order.Status != OrderStatus.IN_DELIVERY)
            return;

        order.Status = OrderStatus.DELIVERED;
        order.AddHistory(OrderStatus.DELIVERED, "Commande livree");
        await _publisher.PublishAsync("OrderCompleted", order.Id.ToString(), new { orderId = order.Id });
    }
}
