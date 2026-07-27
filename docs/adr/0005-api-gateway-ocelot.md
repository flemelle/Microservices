# ADR-0005 — API Gateway avec Ocelot

## Statut
Accepté

## Contexte
Les clients externes (application client, restaurant, livreur) ne doivent pas dépendre de la topologie
interne des microservices, ni appeler directement chaque service. Il faut un point d'entrée unique pour le
routage, et un emplacement naturel pour les préoccupations transverses futures (auth, rate limiting).

## Décision
Utiliser **Ocelot** comme API Gateway, cohérent avec le choix du stack .NET pour tout le prototype (mêmes
outils, mêmes conventions de configuration, pas de composant technologique supplémentaire à opérer).
Configuration déclarative par fichier `ocelot.json` routant chaque préfixe (`/v1/restaurants`, `/v1/catalog`,
`/v1/orders`, `/v1/payments`, `/v1/delivery`) vers le microservice interne correspondant.

## Alternatives envisagées
- **Kong / Express Gateway** : solutions robustes et éprouvées en production, mais introduisent une
  technologie et un langage de configuration supplémentaires par rapport au reste du prototype (100% .NET)
  pour un bénéfice marginal à l'échelle de ce projet pédagogique.
- **Mock simple (reverse proxy minimal fait main)** : aurait suffi pour la démonstration mais n'aurait pas
  permis d'illustrer un vrai outil de Gateway du marché.

## Conséquences
- (+) Un seul point d'entrée exposé sur l'hôte ; les microservices restent uniquement accessibles sur le
  réseau Docker interne.
- (+) Cohérence technologique avec le reste du prototype, courbe d'apprentissage réduite.
- (-) Ocelot est spécifique à l'écosystème .NET (moins pertinent si une partie du système était réécrite
  dans un autre langage) — acceptable ici car tout le prototype est en .NET.
