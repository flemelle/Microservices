using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var log = new ConcurrentQueue<NotificationLog>();
builder.Services.AddSingleton(log);
builder.Services.AddHostedService<DomainEventsConsumer>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

// Endpoint de demo (hors contrat officiel) pour visualiser les notifications simulees envoyees.
app.MapGet("/v1/notifications", (int limit = 50) => Results.Ok(log.Take(limit).Reverse().ToList()));

app.MapGet("/", () => Results.Ok(new { service = "notification-service", status = "up", sent = log.Count }));

app.Run();

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

record NotificationLog(Guid Id, string RecipientType, string RecipientId, string Channel, string Template, string Message, DateTime SentAt);

record OrderCreatedDto(Guid OrderId, Guid CustomerId, Guid RestaurantId, decimal Total);
record OrderConfirmedDto(Guid OrderId, Guid CourierId);
record OrderCancelledDto(Guid OrderId, string? Reason);
record OrderCompletedDto(Guid OrderId);
record PaymentSucceededDto(Guid OrderId, Guid PaymentId, decimal Amount, string Currency);
record PaymentFailedDto(Guid OrderId, string Reason);
record CourierAssignedDto(Guid OrderId, Guid DeliveryId, Guid CourierId, string CourierName);
record DeliveryStatusChangedDto(Guid OrderId, Guid DeliveryId, string Status);
record DeliveryCompletedDto(Guid OrderId, Guid DeliveryId);

/// <summary>
/// Consomme order.events, payment.events et delivery.events (consumer group "notification-service-cg")
/// et simule l'envoi de notifications multi-canal (Email/Push/SMS) aux clients, restaurants et livreurs -
/// voir architecture.md, exigence fonctionnelle "Notifications". Ce service ne pilote jamais le flux
/// metier : une panne ici n'affecte jamais la SAGA de commande (decouplage total par Kafka).
/// </summary>
class DomainEventsConsumer : BackgroundService
{
    private readonly ConcurrentQueue<NotificationLog> _log;
    private readonly ILogger<DomainEventsConsumer> _logger;
    private readonly string _bootstrapServers;
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public DomainEventsConsumer(ConcurrentQueue<NotificationLog> log, ILogger<DomainEventsConsumer> logger)
    {
        _log = log;
        _logger = logger;
        _bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "kafka:9092";
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => Consume(stoppingToken), stoppingToken);

    private void Consume(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "notification-service-cg",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        IConsumer<string, string>? consumer = null;
        while (!stoppingToken.IsCancellationRequested && consumer is null)
        {
            try
            {
                consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe(new[] { "order.events", "payment.events", "delivery.events" });
                _logger.LogInformation("Abonne a order.events, payment.events, delivery.events (groupe notification-service-cg)");
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
            catch (OperationCanceledException) { break; }
            catch (ConsumeException ex) { _logger.LogError(ex, "Erreur de consommation Kafka"); }
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
            var data = root.GetProperty("data");

            switch (eventType)
            {
                case "OrderCreated":
                {
                    var e = data.Deserialize<OrderCreatedDto>(JsonOptions)!;
                    Notify("RESTAURANT", e.RestaurantId.ToString(), "PUSH", "NEW_ORDER", $"Nouvelle commande recue ({e.Total:C})");
                    break;
                }
                case "OrderConfirmed":
                {
                    var e = data.Deserialize<OrderConfirmedDto>(JsonOptions)!;
                    Notify("CUSTOMER", e.OrderId.ToString(), "PUSH", "ORDER_CONFIRMED", "Votre commande est confirmee, un livreur a ete assigne");
                    break;
                }
                case "OrderCancelled":
                {
                    var e = data.Deserialize<OrderCancelledDto>(JsonOptions)!;
                    Notify("CUSTOMER", e.OrderId.ToString(), "EMAIL", "ORDER_CANCELLED", $"Votre commande a ete annulee. {e.Reason}");
                    break;
                }
                case "OrderCompleted":
                {
                    var e = data.Deserialize<OrderCompletedDto>(JsonOptions)!;
                    Notify("CUSTOMER", e.OrderId.ToString(), "PUSH", "ORDER_DELIVERED", "Votre commande a ete livree, bon appetit !");
                    break;
                }
                case "PaymentSucceeded":
                {
                    var e = data.Deserialize<PaymentSucceededDto>(JsonOptions)!;
                    Notify("CUSTOMER", e.OrderId.ToString(), "EMAIL", "PAYMENT_CONFIRMATION", $"Paiement de {e.Amount} {e.Currency} confirme");
                    break;
                }
                case "PaymentFailed":
                {
                    var e = data.Deserialize<PaymentFailedDto>(JsonOptions)!;
                    Notify("CUSTOMER", e.OrderId.ToString(), "SMS", "PAYMENT_FAILED", $"Paiement refuse : {e.Reason}");
                    break;
                }
                case "CourierAssigned":
                {
                    var e = data.Deserialize<CourierAssignedDto>(JsonOptions)!;
                    Notify("COURIER", e.CourierId.ToString(), "PUSH", "DELIVERY_PROPOSAL", $"Nouvelle livraison assignee pour la commande {e.OrderId}");
                    break;
                }
                case "DeliveryStatusChanged":
                {
                    var e = data.Deserialize<DeliveryStatusChangedDto>(JsonOptions)!;
                    Notify("CUSTOMER", e.OrderId.ToString(), "PUSH", "DELIVERY_STATUS", $"Statut de livraison : {e.Status}");
                    break;
                }
                case "DeliveryCompleted":
                {
                    var e = data.Deserialize<DeliveryCompletedDto>(JsonOptions)!;
                    Notify("CUSTOMER", e.OrderId.ToString(), "SMS", "DELIVERY_COMPLETED", "Votre colis a ete remis");
                    break;
                }
                default: break; // tolerant reader
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Impossible de traiter un evenement de notification");
        }
    }

    private void Notify(string recipientType, string recipientId, string channel, string template, string message)
    {
        var entry = new NotificationLog(Guid.NewGuid(), recipientType, recipientId, channel, template, message, DateTime.UtcNow);
        _log.Enqueue(entry);
        while (_log.Count > 500 && _log.TryDequeue(out _)) { } // borne la memoire du log de demo
        Console.WriteLine($"[notification-service] -> {channel} to {recipientType}:{recipientId} [{template}] {message}");
    }
}
