using Polly;
using Polly.Extensions.Http;
using Polly.Timeout;

namespace OrderService.Resilience;

/// <summary>
/// Politiques Polly appliquees a l'appel order-service -> restaurant-service (cf. ADR-0004).
/// Ordre d'enregistrement dans Program.cs (AddPolicyHandler, premier ajoute = plus exterieur) :
/// CircuitBreaker(Retry(Timeout(appel HTTP))).
/// - Timeout : le plus interne, borne chaque tentative individuelle a 2s.
/// - Retry : reessaie jusqu'a 3 fois (backoff 200/400/800ms) une operation Timeout+HTTP en echec transitoire.
/// - CircuitBreaker : le plus externe, ne voit qu'un resultat par operation (deja retryee) ; s'ouvre
///   apres 5 echecs consecutifs de l'operation complete, pour ne pas marteler un restaurant-service
///   deja en difficulte pendant que le circuit est ouvert.
/// </summary>
public static class ResiliencePolicies
{
    public static IAsyncPolicy<HttpResponseMessage> TimeoutPolicy() =>
        Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(2), TimeoutStrategy.Optimistic);

    public static IAsyncPolicy<HttpResponseMessage> RetryPolicy() =>
        HttpPolicyExtensions
            .HandleTransientHttpError()
            .Or<TimeoutRejectedException>()
            .WaitAndRetryAsync(
                retryCount: 3,
                sleepDurationProvider: attempt => TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt - 1)),
                onRetry: (outcome, delay, attempt, _) =>
                    Console.Error.WriteLine($"[order-service] Retry #{attempt} vers restaurant-service dans {delay.TotalMilliseconds}ms ({DescribeOutcome(outcome)})"));

    public static IAsyncPolicy<HttpResponseMessage> CircuitBreakerPolicy() =>
        HttpPolicyExtensions
            .HandleTransientHttpError()
            .Or<TimeoutRejectedException>()
            .CircuitBreakerAsync(
                handledEventsAllowedBeforeBreaking: 5,
                durationOfBreak: TimeSpan.FromSeconds(30),
                onBreak: (outcome, breakDelay) =>
                    Console.Error.WriteLine($"[order-service] Circuit OUVERT vers restaurant-service pendant {breakDelay.TotalSeconds}s ({DescribeOutcome(outcome)})"),
                onReset: () => Console.WriteLine("[order-service] Circuit REFERME vers restaurant-service"),
                onHalfOpen: () => Console.WriteLine("[order-service] Circuit SEMI-OUVERT vers restaurant-service (appel de test)"));

    private static string DescribeOutcome(DelegateResult<HttpResponseMessage> outcome) =>
        outcome.Exception is not null ? outcome.Exception.GetType().Name : $"HTTP {(int)outcome.Result.StatusCode}";
}
