# Choix Architecturaux — Synthèse

Ce document résume les décisions structurantes du projet et leur justification. Le détail complet de
chaque décision (contexte, alternatives envisagées, conséquences) se trouve dans l'ADR correspondant,
dans [`docs/adr/`](adr/).

## Tableau récapitulatif

| # | Choix retenu | Alternative(s) écartée(s) | ADR |
|---|---|---|---|
| 1 | Découpage en microservices par Bounded Context (DDD) | Monolithe modulaire ; découpage ultra-fin par entité | [ADR-0001](adr/0001-decoupage-microservices-ddd.md) |
| 2 | C#/.NET 8 pour l'ensemble du prototype | Python/FastAPI, Node.js/Express, Go | — |
| 3 | Kafka comme broker événementiel | RabbitMQ | [ADR-0002](adr/0002-communication-asynchrone-kafka.md) |
| 4 | SAGA en **orchestration** | SAGA en chorégraphie | [ADR-0003](adr/0003-saga-orchestration.md) |
| 5 | Circuit Breaker + Retry + Timeout + Fallback (Polly) | Retry seul ; aucune protection ; Bulkhead | [ADR-0004](adr/0004-resilience-circuit-breaker-polly.md) |
| 6 | Ocelot comme API Gateway | Kong, Express Gateway, mock simple | [ADR-0005](adr/0005-api-gateway-ocelot.md) |
| 7 | CQRS sur le catalogue (bonus) | Cache Redis simple ; Event Sourcing complet | [ADR-0006](adr/0006-cqrs-catalogue.md) |
| 8 | Pas de base de données partagée (1 stockage logique / service) | BDD partagée avec schémas séparés ; BDD réelle par service dans le prototype | [ADR-0007](adr/0007-pas-de-bdd-partagee.md) |

---

## 1. Découpage en microservices par Bounded Context

**Choix :** un microservice par Bounded Context identifié par analyse DDD (Restaurant, Catalogue,
Commande, Paiement, Livraison, Notification, + Client et Évaluation conçus mais non codés), avec deux
écarts volontaires :
- `catalog-service` séparé de `restaurant-service` (prépare le CQRS, §7 ci-dessous),
- `delivery-service` regroupe Livreur + Livraison (cohésion forte du cycle de vie).

**Pourquoi :** un monolithe modulaire aurait été plus simple mais ne permet pas de démontrer les patterns
exigés par l'énoncé (SAGA inter-services, résilience réseau, CQRS, Kafka). Un découpage ultra-fin (ex.
`menu-service` séparé de `restaurant-service`) aurait au contraire multiplié le couplage synchrone sans
gain de cohésion, le menu n'ayant pas de cycle de vie indépendant du restaurant qui le possède.

## 2. C#/.NET 8 pour l'ensemble du prototype

**Choix :** les 6 microservices et l'API Gateway sont tous écrits en C#/.NET 8 (minimal APIs).

**Pourquoi :** cohérence technologique sur tout le prototype — mêmes outils, mêmes conventions, aucune
rupture de langage entre les services métier et la Gateway. Décision prise en tout début de projet parmi
les options suggérées par l'énoncé (Python/FastAPI, Node.js/Express, Go, C#/.NET).

## 3. Kafka comme broker événementiel

**Choix :** Kafka en mode KRaft (sans Zookeeper), avec topics dédiés par flux, partitionnement par
`orderId`/`restaurantId`, un consumer group par service, garantie de livraison at-least-once.

**Pourquoi :** RabbitMQ aurait été plus simple à opérer pour un projet court (c'est d'ailleurs ce que
suggère l'énoncé), mais Kafka permet de démontrer explicitement les concepts du chapitre 8 (topics,
partitions, consumer groups, garanties de livraison) visés comme extension bonus — un choix assumé sachant
le temps disponible pour l'explorer.

## 4. SAGA en orchestration

**Choix :** `order-service` est l'orchestrateur explicite du processus "passage de commande" : il envoie
des commandes (`ProcessPayment`, `AssignCourier`, `RefundPayment`) via Kafka et réagit aux événements de
résultat pour faire progresser ou compenser la commande.

**Pourquoi :** centralise la logique d'un processus critique (paiement + logistique) en un seul endroit,
ce qui la rend plus facile à tracer, tester et faire évoluer qu'une chorégraphie où chaque service réagit
de façon autonome — au prix d'un couplage de l'orchestrateur vers les participants, jugé acceptable car
`order-service` est déjà le service métier central du domaine Commande.

## 5. Résilience : Circuit Breaker + Retry + Timeout + Fallback (Polly)

**Choix :** l'appel synchrone `order-service → restaurant-service` (validation du menu/des prix avant
création de commande) est protégé par une composition Polly `CircuitBreaker(Retry(Timeout(HTTP)))` :
timeout 2s/tentative, retry ×3 avec backoff exponentiel, ouverture du circuit après 5 échecs consécutifs
pendant 30s, fallback en `503` immédiat.

**Pourquoi :** c'est le seul couplage synchrone fort de l'architecture — sans protection, une panne de
`restaurant-service` bloquerait `order-service` en cascade (épuisement de threads/connexions). L'ordre des
policies (Circuit Breaker au-dessus du Retry) évite qu'un service déjà en difficulté ne soit martelé par
des tentatives répétées ("retry storm").

## 6. Ocelot comme API Gateway

**Choix :** Ocelot route chaque préfixe (`/v1/restaurants`, `/v1/orders`, etc.) vers le microservice
interne correspondant ; seule la Gateway expose un port sur l'hôte.

**Pourquoi :** cohérence avec le stack 100% .NET du prototype — pas de technologie ni de langage de
configuration supplémentaire à opérer (Kong, Express Gateway) pour un bénéfice marginal à l'échelle de ce
projet.

## 7. CQRS sur le catalogue (extension bonus)

**Choix :** `restaurant-service` reste l'unique source de vérité côté écriture ; `catalog-service`
maintient une projection dénormalisée en lecture seule, reconstruite exclusivement à partir des événements
Kafka `restaurant.events` (rejouable depuis `offset=earliest`).

**Pourquoi :** profils de lecture/écriture radicalement différents — peu d'écritures administratives côté
restaurant, beaucoup de lectures de recherche/navigation côté client. Un simple cache Redis aurait été plus
rapide à mettre en place mais n'aurait démontré qu'une optimisation de performance, pas la séparation
complète des responsabilités Commande/Requête. L'Event Sourcing complet (source de vérité événementielle
même côté écriture) a été jugé disproportionné pour le temps imparti.

## 8. Pas de base de données partagée

**Choix :** chaque service possède son propre stockage logique (en mémoire dans le prototype, conformément
à la consigne « pas besoin de BDD réelle ») ; aucune jointure ni foreign key inter-service.

**Pourquoi :** une BDD partagée est un anti-pattern classique en microservices — elle recrée un couplage
fort au niveau du schéma et un point de défaillance commun. Une BDD réelle par service (ex. un conteneur
Postgres par service) a été envisagée mais écartée pour ce prototype afin de rester focalisé sur la
démonstration des patterns d'architecture (SAGA, CQRS, résilience) plutôt que sur des préoccupations de
persistance hors sujet.

---

*Document généré comme synthèse de navigation ; pour l'analyse complète (contexte, alternatives,
conséquences) de chaque décision, se référer à l'ADR correspondant dans [`docs/adr/`](adr/).*
