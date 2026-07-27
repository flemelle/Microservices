using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));

var kafkaBootstrap = PaymentProcessor.GetKafkaBootstrap();

var payments = new ConcurrentDictionary<Guid, Payment>();
var paymentsByOrder = new ConcurrentDictionary<Guid, Guid>();
var chaos = new PaymentChaosState();

builder.Services.AddSingleton(payments);
builder.Services.AddSingleton(paymentsByOrder);
builder.Services.AddSingleton(chaos);
builder.Services.AddSingleton(new PaymentEventPublisher(kafkaBootstrap));
builder.Services.AddHostedService<PaymentCommandsConsumer>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

var publisher = app.Services.GetRequiredService<PaymentEventPublisher>();

// Endpoint de demonstration/debogage - le flux nominal de la SAGA passe par payment.commands (Kafka).
app.MapPost("/v1/payments", (PaymentRequest req) =>
{
    var payment = PaymentProcessor.Process(req.OrderId, req.Amount, req.Currency ?? "EUR", payments, paymentsByOrder, chaos);
    _ = PaymentProcessor.PublishResult(payment, publisher);
    return Results.Accepted(value: payment);
});

app.MapGet("/v1/payments/{id:guid}", (Guid id) =>
    FindPayment(id) is { } p ? Results.Ok(p) : Results.NotFound(Error("NOT_FOUND", "Paiement introuvable")));

app.MapGet("/v1/payments/order/{orderId:guid}", (Guid orderId) =>
    FindPaymentByOrder(orderId) is { } p ? Results.Ok(p) : Results.NotFound(Error("NOT_FOUND", "Paiement introuvable pour cette commande")));

app.MapPost("/v1/payments/{id:guid}/refund", (Guid id, RefundRequest? req) =>
{
    if (FindPayment(id) is not { } payment)
        return Results.NotFound(Error("NOT_FOUND", "Paiement introuvable"));

    if (payment.Status != PaymentStatus.CAPTURED)
        return Results.Json(Error("INVALID_STATE", "Paiement non capture, remboursement impossible"), statusCode: StatusCodes.Status409Conflict);

    PaymentProcessor.Refund(payment, req?.Amount, publisher);
    return Results.Accepted();
});

// Pilotage du mode chaos pour la demo (forcer un echec de paiement -> declenche la compensation SAGA).
app.MapPost("/v1/_chaos/force-next-failure", () => { chaos.ForceNextFailure = true; return Results.Ok(new { chaos.ForceNextFailure }); });
app.MapPost("/v1/_chaos/reset", () => { chaos.ForceNextFailure = false; return Results.Ok(new { chaos.ForceNextFailure }); });

app.MapGet("/", () => Results.Ok(new { service = "payment-service", status = "up" }));

app.Run();

static object Error(string code, string message) => new { error = new { code, message } };

Payment? FindPayment(Guid id) => payments.TryGetValue(id, out var p) ? p : null;
Payment? FindPaymentByOrder(Guid orderId) => paymentsByOrder.TryGetValue(orderId, out var id) ? FindPayment(id) : null;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

enum PaymentStatus { AUTHORIZED, CAPTURED, FAILED, REFUNDED, PARTIALLY_REFUNDED }

class Payment
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public decimal Amount { get; set; }
    public string Currency { get; set; } = "EUR";
    public PaymentStatus Status { get; set; }
    public string Provider { get; set; } = "mock-gateway";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

record PaymentRequest(Guid OrderId, decimal Amount, string? Currency);
record RefundRequest(decimal? Amount);

class PaymentChaosState
{
    // Permet de forcer volontairement un echec de paiement pendant la demo pour illustrer la compensation SAGA.
    public bool ForceNextFailure { get; set; }
    private readonly Random _random = new();
    public double FailureRate { get; } = double.TryParse(Environment.GetEnvironmentVariable("PAYMENT_FAILURE_RATE"), out var r) ? r : 0.15;
    public bool ShouldFail()
    {
        if (ForceNextFailure) { ForceNextFailure = false; return true; }
        return _random.NextDouble() < FailureRate;
    }
}

static class PaymentProcessor
{
    public static string GetKafkaBootstrap() => Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "kafka:9092";

    public static Payment Process(Guid orderId, decimal amount, string currency,
        ConcurrentDictionary<Guid, Payment> payments, ConcurrentDictionary<Guid, Guid> paymentsByOrder, PaymentChaosState chaos)
    {
        var payment = new Payment
        {
            Id = Guid.NewGuid(),
            OrderId = orderId,
            Amount = amount,
            Currency = currency,
            Status = chaos.ShouldFail() ? PaymentStatus.FAILED : PaymentStatus.CAPTURED
        };
        payments[payment.Id] = payment;
        paymentsByOrder[orderId] = payment.Id;
        return payment;
    }

    public static async Task PublishResult(Payment payment, PaymentEventPublisher publisher)
    {
        if (payment.Status == PaymentStatus.CAPTURED)
        {
            await publisher.PublishAsync("PaymentSucceeded", payment.OrderId.ToString(),
                new { orderId = payment.OrderId, paymentId = payment.Id, amount = payment.Amount, currency = payment.Currency });
        }
        else
        {
            await publisher.PublishAsync("PaymentFailed", payment.OrderId.ToString(),
                new { orderId = payment.OrderId, reason = "Paiement refuse par la passerelle (mock)" });
        }
    }

    /// <summary>Calcule et applique un remboursement (partiel ou total), puis publie PaymentRefunded.</summary>
    public static void Refund(Payment payment, decimal? amount, PaymentEventPublisher publisher)
    {
        var refundAmount = amount ?? payment.Amount;
        payment.Status = refundAmount >= payment.Amount ? PaymentStatus.REFUNDED : PaymentStatus.PARTIALLY_REFUNDED;
        _ = publisher.PublishAsync("PaymentRefunded", payment.OrderId.ToString(),
            new { orderId = payment.OrderId, paymentId = payment.Id, amount = refundAmount });
    }
}

/// <summary>Publie sur le topic Kafka "payment.events" (PaymentSucceeded/PaymentFailed/PaymentRefunded).</summary>
class PaymentEventPublisher : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private const string Topic = "payment.events";

    public PaymentEventPublisher(string bootstrapServers)
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
            Console.Error.WriteLine($"[payment-service] Echec de publication Kafka ({eventType}): {ex.Message}");
        }
    }

    public void Dispose() => _producer.Flush(TimeSpan.FromSeconds(5));
}

/// <summary>
/// Consomme le topic Kafka "payment.commands" (consumer group "payment-service-cg") : ProcessPayment / RefundPayment.
/// Etape de la SAGA orchestree par order-service (cf. architecture.md §7 et ADR-0003).
/// </summary>
class PaymentCommandsConsumer : BackgroundService
{
    private readonly ConcurrentDictionary<Guid, Payment> _payments;
    private readonly ConcurrentDictionary<Guid, Guid> _paymentsByOrder;
    private readonly PaymentChaosState _chaos;
    private readonly PaymentEventPublisher _publisher;
    private readonly ILogger<PaymentCommandsConsumer> _logger;
    private readonly string _bootstrapServers;
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public PaymentCommandsConsumer(ConcurrentDictionary<Guid, Payment> payments, ConcurrentDictionary<Guid, Guid> paymentsByOrder,
        PaymentChaosState chaos, PaymentEventPublisher publisher, ILogger<PaymentCommandsConsumer> logger)
    {
        _payments = payments;
        _paymentsByOrder = paymentsByOrder;
        _chaos = chaos;
        _publisher = publisher;
        _logger = logger;
        _bootstrapServers = PaymentProcessor.GetKafkaBootstrap();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => Consume(stoppingToken), stoppingToken);

    private void Consume(CancellationToken stoppingToken)
    {
        // Build()/Subscribe() sont des appels locaux (aucune I/O reseau immediate) : librdkafka gere
        // en interne la (re)connexion aux brokers de facon asynchrone. Inutile de les entourer d'une
        // boucle de retry - seule la boucle de consommation ci-dessous a besoin d'un try/catch.
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "payment-service-cg",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        }).Build();
        consumer.Subscribe("payment.commands");
        _logger.LogInformation("Abonne au topic payment.commands (groupe payment-service-cg)");

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
    }

    private async Task Handle(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var eventType = root.GetProperty("eventType").GetString();
            var data = root.GetProperty("data");

            switch (eventType)
            {
                case "ProcessPayment":
                {
                    var cmd = data.Deserialize<ProcessPaymentCommand>(JsonOptions)!;

                    // Idempotence : si un paiement existe deja pour cette commande, ne pas le retraiter
                    // (protege contre les livraisons dupliquees inherentes a la garantie at-least-once, cf. ADR-0002).
                    if (_paymentsByOrder.ContainsKey(cmd.OrderId))
                    {
                        _logger.LogInformation("ProcessPayment ignore (deja traite) pour la commande {OrderId}", cmd.OrderId);
                        return;
                    }

                    var payment = PaymentProcessor.Process(cmd.OrderId, cmd.Amount, cmd.Currency ?? "EUR", _payments, _paymentsByOrder, _chaos);
                    await PaymentProcessor.PublishResult(payment, _publisher);
                    break;
                }
                case "RefundPayment":
                {
                    var cmd = data.Deserialize<RefundPaymentCommand>(JsonOptions)!;
                    if (FindPaymentByOrder(cmd.OrderId) is not { } payment)
                    {
                        _logger.LogWarning("RefundPayment recu pour une commande sans paiement connu {OrderId}", cmd.OrderId);
                        return;
                    }
                    if (payment.Status is PaymentStatus.REFUNDED or PaymentStatus.PARTIALLY_REFUNDED)
                    {
                        return; // deja rembourse (idempotence)
                    }
                    PaymentProcessor.Refund(payment, cmd.Amount, _publisher);
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Impossible de traiter une commande payment.commands");
        }
    }

    private Payment? FindPaymentByOrder(Guid orderId) =>
        _paymentsByOrder.TryGetValue(orderId, out var id) && _payments.TryGetValue(id, out var p) ? p : null;
}

record ProcessPaymentCommand(Guid OrderId, decimal Amount, string? Currency);
record RefundPaymentCommand(Guid OrderId, decimal? Amount);
