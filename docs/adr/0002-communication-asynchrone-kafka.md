# ADR-0002 — Kafka comme broker événementiel pour la communication asynchrone

## Statut
Accepté

## Contexte
Plusieurs interactions inter-services (propagation d'état pour CQRS, orchestration de la SAGA de commande,
notifications) doivent être découplées temporellement et tolérer les pannes transitoires d'un service sans
perdre de message. Il faut choisir un mode de communication asynchrone et un broker.

## Décision
Utiliser **Kafka** comme broker événementiel, avec :
- un topic par flux logique (`restaurant.events`, `payment.commands`, `payment.events`,
  `delivery.commands`, `delivery.events`, `order.events`),
- partitionnement par `orderId` (ou `restaurantId`) pour garantir l'ordre par agrégat,
- un **consumer group dédié par service**,
- garantie de livraison **at-least-once** avec `acks=all` sur les topics critiques et consommateurs
  idempotents (voir `architecture.md` §4.2).

## Alternatives envisagées
- **RabbitMQ** : plus simple à opérer pour un projet court (suggéré par l'énoncé pour cette raison), mais
  moins adapté pour démontrer explicitement les concepts *topics/partitions/consumer groups* du chapitre 8
  (extension bonus visée). Kafka a été choisi car le groupe souhaite explorer cette extension et dispose du
  temps nécessaire.
- **Appels REST synchrones partout** : rejeté — créerait un couplage temporel fort (tous les services
  doivent être up simultanément) et rendrait la SAGA fragile face aux pannes transitoires, en plus d'aller à
  l'encontre du principe de découplage recherché pour la propagation d'état (CQRS).
- **Exactly-once (transactions Kafka)** : jugé disproportionné pour la volumétrie/criticité de ce projet
  pédagogique ; l'idempotence applicative suffit à absorber les doublons d'un at-least-once.

## Conséquences
- (+) Découplage fort, rejouabilité des événements (utile notamment pour reconstruire la projection CQRS
  du catalogue), scalabilité horizontale par consumer group.
- (+) Journal durable : permet de rejouer `restaurant.events` depuis `offset=earliest` pour reconstruire
  `catalog-service` en cas de perte.
- (-) Complexité d'exploitation supérieure à RabbitMQ (configuration KRaft, gestion des partitions) —
  mitigée en développement par une image Docker Compose prête à l'emploi et un seul broker.
- (-) Nécessite une discipline de conception (idempotence, tolerant reader) côté consommateurs.
