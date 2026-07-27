# ADR-0004 — Résilience de l'appel synchrone order-service → restaurant-service (Polly)

## Statut
Accepté

## Contexte
`order-service` doit valider synchroniquement le menu et les prix auprès de `restaurant-service` avant de
créer une commande. C'est le seul appel REST inter-service à fort risque de blocage en cascade : si
`restaurant-service` devient lent ou indisponible, chaque requête entrante sur `order-service` peut rester
bloquée en attente, épuisant ses ressources (threads/connexions) et propageant la panne.

## Décision
Protéger cet appel avec la bibliothèque **Polly** (.NET) en combinant quatre patterns :
1. **Timeout** (2s par tentative),
2. **Retry** (3 tentatives, backoff exponentiel 200/400/800ms) sur erreurs transitoires,
3. **Circuit Breaker** (ouverture après 5 échecs consécutifs, 30s avant repassage en semi-ouvert),
4. **Fallback** (réponse `503` immédiate et explicite au lieu de laisser la requête s'éterniser).

## Alternatives envisagées
- **Retry seul, sans Circuit Breaker** : rejeté — en cas de panne prolongée de `restaurant-service`,
  chaque nouvelle requête client relancerait quand même 3 tentatives + timeouts, aggravant la charge sur un
  service déjà en difficulté (« retry storm »). Le Circuit Breaker coupe court à ce risque.
- **Aucune protection (appel direct)** : rejeté — c'est précisément le scénario de panne en cascade que
  l'architecture microservices doit éviter ; contraire à l'exigence explicite de résilience de l'énoncé.
- **Bulkhead (isolation de pool de threads) en plus** : envisagé mais jugé hors périmètre du prototype
  pédagogique (un seul appel synchrone à protéger, pas de multiplicité de dépendances justifiant
  l'isolation de ressources).

## Conséquences
- (+) Une panne de `restaurant-service` dégrade proprement l'expérience (message clair, réponse rapide) au
  lieu de faire planter ou geler `order-service`.
- (+) Le Circuit Breaker laisse le temps à `restaurant-service` de récupérer sans être bombardé de requêtes
  pendant qu'il est déjà en difficulté.
- (-) Pendant que le circuit est ouvert, aucune commande ne peut être créée même si `restaurant-service` est
  en fait redevenu disponible avant la fin des 30s (compromis assumé du pattern Circuit Breaker).
