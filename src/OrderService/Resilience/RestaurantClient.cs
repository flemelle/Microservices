using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Polly.CircuitBreaker;

namespace OrderService.Resilience;

public record ValidatedItem(Guid MenuItemId, string Name, decimal Price, bool Available);

public record RestaurantValidationResult(
    bool CallSucceeded,
    bool RestaurantFound,
    bool RestaurantOpen,
    bool AllItemsAvailable,
    List<ValidatedItem> Items,
    string? ErrorMessage);

file record ValidationResponseDto(bool RestaurantOpen, bool AllItemsAvailable, List<ValidatedItemDto> Items);
file record ValidatedItemDto(Guid MenuItemId, string Name, decimal Price, bool Available);

public interface IRestaurantClient
{
    Task<RestaurantValidationResult> ValidateAsync(Guid restaurantId, IEnumerable<Guid> menuItemIds, CancellationToken ct);
}

/// <summary>
/// Appel synchrone resilient vers restaurant-service (Timeout + Retry + Circuit Breaker + Fallback),
/// voir architecture.md §8 et ADR-0004. Les policies Polly sont enregistrees sur le HttpClient nomme
/// dans Program.cs ; cette classe se contente d'interpreter le resultat (succes, echec metier, echec
/// de resilience) sans jamais laisser une exception remonter jusqu'a l'appelant.
/// </summary>
public class RestaurantClient : IRestaurantClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly HttpClient _httpClient;
    private readonly ILogger<RestaurantClient> _logger;

    public RestaurantClient(HttpClient httpClient, ILogger<RestaurantClient> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    public async Task<RestaurantValidationResult> ValidateAsync(Guid restaurantId, IEnumerable<Guid> menuItemIds, CancellationToken ct)
    {
        var itemsParam = string.Join(',', menuItemIds);

        try
        {
            var response = await _httpClient.GetAsync($"/v1/restaurants/{restaurantId}/validate?items={itemsParam}", ct);

            if (response.StatusCode == HttpStatusCode.NotFound)
                return new RestaurantValidationResult(true, false, false, false, new(), "Restaurant introuvable");

            // 200 (valide) ou 409 (ferme / item indisponible) portent tous deux le meme corps ValidationResult.
            var body = await response.Content.ReadFromJsonAsync<ValidationResponseDto>(JsonOptions, cancellationToken: ct);
            if (body is null)
                return new RestaurantValidationResult(false, false, false, false, new(), "Reponse invalide de restaurant-service");

            return new RestaurantValidationResult(
                CallSucceeded: true,
                RestaurantFound: true,
                RestaurantOpen: body.RestaurantOpen,
                AllItemsAvailable: body.AllItemsAvailable,
                Items: body.Items.Select(i => new ValidatedItem(i.MenuItemId, i.Name, i.Price, i.Available)).ToList(),
                ErrorMessage: null);
        }
        catch (BrokenCircuitException)
        {
            _logger.LogWarning("Circuit ouvert vers restaurant-service : appel court-circuite (fallback immediat)");
            return new RestaurantValidationResult(false, false, false, false, new(), "restaurant-service indisponible (circuit breaker ouvert)");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Echec de l'appel resilient a restaurant-service apres retries/timeout");
            return new RestaurantValidationResult(false, false, false, false, new(), "restaurant-service indisponible");
        }
    }
}
