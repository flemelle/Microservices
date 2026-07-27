# ADR-0006 — CQRS sur le Catalogue (extension bonus)

## Statut
Accepté

## Contexte
Le catalogue (recherche/consultation de restaurants et de menus) a un profil d'usage très asymétrique :
très peu d'écritures (un restaurateur modifie son menu occasionnellement) contre un très grand nombre de
lectures (chaque client parcourt/recherche en permanence). Un modèle unique optimisé pour l'écriture
transactionnelle (`restaurant-service`) desservirait mal ce besoin de lecture à fort volume, filtré et
potentiellement agrégé (recherche par localisation, cuisine, plat).

## Décision
Séparer explicitement **côté Commande** (`restaurant-service`, source de vérité, modèle normalisé) et
**côté Requête** (`catalog-service`, projection dénormalisée en lecture seule), synchronisés par les
événements `restaurant.events` publiés sur Kafka. `catalog-service` ne possède aucune écriture exposée aux
clients : sa seule source de vérité pour se mettre à jour est le flux d'événements.

## Alternatives envisagées
- **Un seul service `restaurant-service` avec cache de lecture interne (ex. Redis)** : plus simple, et
  envisagé comme extension bonus alternative, mais ne démontre pas la séparation architecturale complète
  Commande/Requête ni la reconstruction par rejeu d'événements ; se limite à une optimisation de
  performance plutôt qu'à un changement de modèle de responsabilité.
- **Event Sourcing complet** (stocker uniquement le journal d'événements comme source de vérité, y compris
  côté écriture) : jugé disproportionné pour le temps imparti — le côté écriture (`restaurant-service`)
  reste en modèle d'état classique (CRUD), seul le côté lecture est reconstruit par événements.

## Conséquences
- (+) Le catalogue peut être mis à l'échelle et optimisé (index de recherche, cache) indépendamment du
  service d'écriture, sans risque de dégrader les performances d'administration du restaurant.
- (+) Reconstruction possible du read-model par rejeu complet de `restaurant.events` (`offset=earliest`) —
  robustesse en cas de corruption/perte de la projection.
- (-) Cohérence éventuelle assumée : un changement de menu peut mettre jusqu'à quelques centaines de
  millisecondes à apparaître dans le catalogue (latence de propagation Kafka). Documenté et jugé acceptable
  car ce n'est pas une donnée financière ou transactionnelle.
