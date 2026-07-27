# ADR-0007 — Une base de données logique par service, pas de BDD partagée

## Statut
Accepté

## Contexte
Une base de données partagée entre plusieurs microservices est un anti-pattern classique : elle recrée un
couplage fort au niveau du schéma (un changement de colonne dans un service peut casser un autre service),
contourne l'encapsulation métier de chaque service, et empêche de faire évoluer indépendamment les
technologies de stockage.

## Décision
Chaque microservice possède son **propre stockage logique**, jamais partagé ni accédé directement par un
autre service. Toute donnée nécessaire à un service mais possédée par un autre est obtenue soit par appel
API synchrone (lecture ponctuelle), soit par réplication via événements (lecture fréquente, ex. projection
`catalog-service`). Dans le prototype, le stockage est simulé **en mémoire** (dictionnaires thread-safe)
pour chaque service, conformément à la consigne « pas besoin de BDD réelle » ; en production, chaque service
choisirait la technologie adaptée à son usage (ex. PostgreSQL pour `order-service`/`payment-service` à
cohérence forte, un store documentaire pour `catalog-service` à lecture dénormalisée).

## Alternatives envisagées
- **BDD partagée avec schémas séparés par service** : rejetée — réduit le couplage de nommage mais laisse
  subsister un couplage opérationnel fort (une seule instance à faire évoluer/migrer/scaler pour tous les
  services, un incident sur l'instance impacte tout le système).
- **BDD réelle par service dans le prototype** (ex. un conteneur Postgres par service) : envisagé, mais
  écarté pour ce prototype afin de rester focalisé sur la démonstration des patterns d'architecture
  (SAGA, CQRS, résilience) sans complexifier le `docker-compose.yml` avec des préoccupations de persistance
  hors sujet — explicitement permis par l'énoncé (« pas besoin de BDD réelle »).

## Conséquences
- (+) Autonomie totale de chaque service sur son modèle de données interne.
- (+) `docker-compose.yml` du prototype reste simple (pas de conteneur de base de données à gérer).
- (-) Les données en mémoire ne survivent pas au redémarrage d'un conteneur — acceptable pour une
  démonstration, à corriger en production par un vrai moteur de persistance par service.
