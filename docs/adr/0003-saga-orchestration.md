# ADR-0003 — SAGA en orchestration pour le passage de commande

## Statut
Accepté

## Contexte
Le passage de commande engage trois services indépendants (Commande, Paiement, Livraison) sans base de
données partagée. Il faut garantir la cohérence globale du processus (annuler/rembourser proprement en cas
d'échec d'une étape) sans transaction distribuée classique (2PC), écartée pour son couplage fort et son
incompatibilité avec la disponibilité recherchée dans une architecture microservices.

## Décision
Implémenter le processus comme une **SAGA en orchestration**, portée par `order-service` : ce service
connaît explicitement la séquence des étapes (paiement puis assignation livreur), envoie des commandes aux
autres services via Kafka (`payment.commands`, `delivery.commands`) et réagit à leurs événements de
résultat (`payment.events`, `delivery.events`) pour faire progresser ou compenser la commande.

## Alternatives envisagées
- **Chorégraphie** (chaque service réagit aux événements des autres sans chef d'orchestre) : plus découplée
  et plus « pure » événementiellement, mais la logique du processus se retrouve diffusée entre plusieurs
  services, ce qui la rend plus difficile à tracer, tester et faire évoluer — un point important pour un
  processus aussi critique que la commande (paiement + logistique). Rejetée pour ce processus précis ; reste
  pertinente pour des réactions simples à un seul événement (ex. `notification-service` réagissant à
  plusieurs événements sans piloter de processus).
- **Transaction distribuée 2PC** : rejetée — nécessiterait un coordinateur bloquant, incompatible avec
  l'autonomie et la disponibilité des microservices, et avec l'appel à une passerelle de paiement externe
  qui ne participe pas à un protocole 2PC.

## Conséquences
- (+) Logique du processus centralisée et lisible dans `order-service` (machine à état explicite,
  facilement testable et traçable dans les logs/diagramme de séquence).
- (+) Ajout d'une nouvelle étape (ex. vérification anti-fraude) ne nécessite de modifier que
  l'orchestrateur.
- (-) `order-service` doit connaître les commandes/événements de `payment-service` et `delivery-service`
  → couplage de l'orchestrateur vers les participants (acceptable : `order-service` est déjà le service
  métier central du domaine Commande).
- (-) Nécessite une gestion explicite des timeouts et de l'idempotence côté orchestrateur (ex. ne pas
  redéclencher un remboursement si `NoCourierAvailable` est livré deux fois).
