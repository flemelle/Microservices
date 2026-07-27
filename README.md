# Plateforme de Livraison de Repas — Architecture Microservices

Projet de conception d'une architecture microservices pour une plateforme de livraison de repas
(type Uber Eats / Deliveroo), réalisé dans le cadre du cours d'architecture microservices.

> **Documentation complète** : [`docs/architecture.md`](docs/architecture.md) (description générale,
> découpage DDD, communication inter-services, SAGA, résilience, CQRS, diagrammes Mermaid).
> **ADRs** : [`docs/adr/`](docs/adr/). **Contrats OpenAPI** : [`docs/api/`](docs/api/).

## Stack

- **Langage** : C# / .NET 8 (minimal APIs)
- **Broker événementiel** : Apache Kafka (mode KRaft)
- **API Gateway** : Ocelot
- **Résilience** : Polly (Circuit Breaker, Retry, Timeout, Fallback)
- **Conteneurisation** : Docker Compose
- **Stockage** : en mémoire (mock, cf. [ADR-0007](docs/adr/0007-pas-de-bdd-partagee.md))

## Structure du dépôt

```
docs/                    Documentation d'architecture, ADRs, contrats OpenAPI
src/
  RestaurantService/      Profil restaurant, menus, publie sur restaurant.events (CQRS write side)
  CatalogService/          Projection de lecture (CQRS read side), consumer Kafka
  OrderService/            Orchestrateur de la SAGA "passage de commande", client resilient (Polly)
  PaymentService/          Paiement mocke, remboursements
  DeliveryService/         Livreurs, assignation, tracking simule
  NotificationService/     Notifications simulees (Email/Push/SMS)
  ApiGateway/              Ocelot, point d'entree unique
docker-compose.yml        Lance Kafka + Kafka UI + les 6 microservices + la gateway
presentation/slides.md    Support de presentation (format Marp)
```

## Lancer le prototype

Pré-requis : **Docker** et **Docker Compose** (aucune installation locale de .NET n'est nécessaire, la
compilation se fait dans les conteneurs).

```bash
docker compose up --build
```

Premier démarrage un peu plus long (téléchargement des images .NET SDK/ASP.NET et build de 7 conteneurs).
Attendre que `kafka` soit en état `healthy` avant que les services applicatifs ne se connectent
(géré automatiquement par les `depends_on: condition: service_healthy` du `docker-compose.yml`).

### Points d'accès une fois lancé

| Service | URL directe (debug/Swagger) | Via l'API Gateway |
|---|---|---|
| API Gateway | http://localhost:8080 | — |
| restaurant-service | http://localhost:8081/swagger | http://localhost:8080/v1/restaurants |
| catalog-service | http://localhost:8082/swagger | http://localhost:8080/v1/catalog |
| order-service | http://localhost:8083/swagger | http://localhost:8080/v1/orders |
| payment-service | http://localhost:8084/swagger | http://localhost:8080/v1/payments |
| delivery-service | http://localhost:8085/swagger | http://localhost:8080/v1/delivery |
| notification-service | http://localhost:8086/swagger | http://localhost:8080/v1/notifications |
| Kafka UI (topics/partitions/consumer groups) | http://localhost:8090 | — |

Deux restaurants et leurs menus sont pré-chargés au démarrage de `restaurant-service` :

- `11111111-1111-1111-1111-111111111111` — Pizzeria Bella Napoli
  - `aaaaaaaa-0000-0000-0000-000000000001` — Pizza Margherita (11.50€)
  - `aaaaaaaa-0000-0000-0000-000000000002` — Pizza Regina (13.00€)
- `22222222-2222-2222-2222-222222222222` — Sushi Sakura
  - `bbbbbbbb-0000-0000-0000-000000000001` — Plateau California x12 (15.90€)
  - `bbbbbbbb-0000-0000-0000-000000000002` — Ramen Tonkotsu (12.90€)

## Scénario de démonstration

Tous les appels ci-dessous passent par l'**API Gateway** (`localhost:8080`). Ouvrir en parallèle
**Kafka UI** (http://localhost:8090 → Topics) pour observer les messages transiter sur
`order.events`, `payment.commands`, `payment.events`, `delivery.commands`, `delivery.events`,
`restaurant.events`, et les logs des conteneurs (`docker compose logs -f order-service`) pour suivre
la SAGA en direct.

### 1. Happy path — commande, paiement, livraison

```bash
curl -s -X POST http://localhost:8080/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "33333333-3333-3333-3333-333333333333",
    "restaurantId": "11111111-1111-1111-1111-111111111111",
    "items": [ { "menuItemId": "aaaaaaaa-0000-0000-0000-000000000001", "quantity": 2 } ],
    "deliveryAddress": { "street": "1 rue de Paris", "city": "Paris" }
  }' | tee /tmp/order.json

ORDER_ID=$(jq -r .id /tmp/order.json)

# Suivre la progression de la SAGA (rejouer toutes les ~2s) :
watch -n 2 "curl -s http://localhost:8080/v1/orders/$ORDER_ID/status | jq"
```

Statuts attendus dans l'ordre : `CREATED` → `AWAITING_PAYMENT` → `PAID` → `AWAITING_COURIER` →
`CONFIRMED` → `IN_PREPARATION` → `IN_DELIVERY` → `DELIVERED` (les deux dernières étapes prennent
~10s, simulées par `delivery-service`). Consulter `GET /v1/notifications` sur
`notification-service` pour voir les notifications simulées envoyées à chaque étape.

### 2. Compensation SAGA — paiement refusé

Forcer un échec de paiement puis repasser une commande :

```bash
curl -s -X POST http://localhost:8084/v1/_chaos/force-next-failure
curl -s -X POST http://localhost:8080/v1/orders -H "Content-Type: application/json" -d '{ ... }'
```

Résultat attendu : `AWAITING_PAYMENT` → `CANCELLED` (aucune compensation nécessaire, rien n'avait
encore été engagé — voir [architecture.md §7.4](docs/architecture.md#74-scénarios-déchec-et-compensation)).

### 3. Compensation SAGA — aucun livreur disponible (remboursement)

```bash
curl -s -X POST http://localhost:8085/v1/_chaos/force-no-courier
curl -s -X POST http://localhost:8080/v1/orders -H "Content-Type: application/json" -d '{ ... }'
```

Résultat attendu : `AWAITING_COURIER` → (RefundPayment publié sur `payment.commands`) →
`PaymentRefunded` reçu → `CANCELLED`. C'est la **compensation** de bout en bout du diagramme de
séquence [§12.3](docs/architecture.md#123-séquence--saga-passage-de-commande-happy-path-et-compensation).

### 4. Pattern de résilience — Circuit Breaker sur order-service → restaurant-service

```bash
# Active une panne simulee de restaurant-service (500 + latence de 3s sur /validate)
curl -s -X POST "http://localhost:8081/v1/_chaos/enable?delayMs=3000"

# Rejouer plusieurs creations de commande : les 5 premieres tentent Retry (voir logs order-service),
# puis le circuit s'ouvre et les suivantes echouent instantanement en 503.
for i in {1..7}; do
  curl -s -o /dev/null -w "tentative $i -> HTTP %{http_code} en %{time_total}s\n" \
    -X POST http://localhost:8080/v1/orders -H "Content-Type: application/json" -d '{ ... }'
done

# Observer dans les logs : "[order-service] Circuit OUVERT vers restaurant-service pendant 30s"
docker compose logs order-service | grep -i circuit

# Desactiver la panne simulee puis attendre 30s : le circuit repasse en semi-ouvert puis se referme.
curl -s -X POST http://localhost:8081/v1/_chaos/disable
```

Voir le détail de ce comportement dans le diagramme de séquence
[§12.4](docs/architecture.md#124-séquence--circuit-breaker-sur-lappel-order-service--restaurant-service)
et [ADR-0004](docs/adr/0004-resilience-circuit-breaker-polly.md).

### 5. CQRS — propagation catalogue

```bash
curl -s -X POST http://localhost:8080/v1/restaurants/11111111-1111-1111-1111-111111111111/menu \
  -H "Content-Type: application/json" \
  -d '{ "name": "Pizza 4 Fromages", "description": "Mozzarella, gorgonzola, parmesan, chevre", "price": 14.50 }'

# Quelques centaines de ms plus tard, le nouveau plat apparait dans la projection catalogue :
curl -s http://localhost:8080/v1/catalog/restaurants/11111111-1111-1111-1111-111111111111 | jq
```

## Arrêter l'environnement

```bash
docker compose down
```

## Limites connues du prototype (assumées, cf. [architecture.md §13](docs/architecture.md#13-périmètre-du-prototype))

- Stockage en mémoire : les données ne survivent pas à un redémarrage de conteneur.
- `customer-service` et `review-service` sont conçus (bounded context, modèle de données, position
  dans l'architecture) mais non codés — non nécessaires pour démontrer les patterns exigés.
- Pas d'authentification, pas de paiement réel, pas d'UI graphique (démonstration via HTTP/Swagger).

## Note sur la génération de ce projet

Ce prototype (documentation + code) a été rédigé avec l'assistance de Claude (Anthropic) dans un
environnement sans SDK .NET ni Docker installés : le code n'a donc **pas pu être compilé ni exécuté**
avant ce commit. Il a été relu attentivement (cohérence des contrats JSON entre services, signatures
d'API .NET/Polly/Confluent.Kafka), mais un premier `docker compose up --build` doit être fait **avant
la soutenance** pour corriger d'éventuelles erreurs de compilation ou de configuration qui n'auraient
pas pu être détectées par relecture seule.
