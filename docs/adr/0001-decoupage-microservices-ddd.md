# ADR-0001 — Découpage en microservices selon les Bounded Contexts DDD

## Statut
Accepté

## Contexte
Le domaine « livraison de repas » comporte plusieurs sous-domaines aux règles de gestion et aux rythmes
d'évolution différents (Restaurant, Commande, Paiement, Livraison, Client, Évaluation, Notification). Il
faut décider d'un découpage en microservices qui limite le couplage tout en restant gérable dans le cadre
d'un projet pédagogique de ~15h/étudiant.

## Décision
Découper selon les **Bounded Contexts** identifiés par analyse DDD (voir `architecture.md` §2), avec un
service par contexte sauf deux écarts justifiés :
- `restaurant-service` (write) et `catalog-service` (read) séparés pour préparer un CQRS sur un besoin de
  lecture à fort volume et à modèle différent de l'écriture.
- `delivery-service` regroupe Livreur + Livraison (cohésion forte du cycle de vie, éviter un couplage
  synchrone permanent entre deux services trop fins).

## Alternatives envisagées
- **Un seul monolithe modulaire** : plus simple à développer en 15h, mais ne permet pas de démontrer les
  patterns exigés (SAGA inter-services, résilience réseau, CQRS, Kafka). Rejeté car hors sujet de l'énoncé.
- **Découpage ultra-fin** (un microservice par entité, ex. `menu-service` séparé de `restaurant-service`) :
  rejeté, sur-découpage qui aurait multiplié le couplage synchrone sans bénéfice de cohésion (le menu n'a
  pas de cycle de vie ni de rythme d'évolution indépendant du restaurant qui le possède).

## Conséquences
- (+) Chaque service a une responsabilité claire et un modèle de données propre, sans base partagée.
- (+) Le découpage permet de démontrer clairement SAGA, résilience et CQRS avec un nombre de services
  raisonnable (6 implémentés + 2 documentés).
- (-) Complexité opérationnelle plus élevée qu'un monolithe (multiplie les déploiements, la supervision, les
  contrats d'API à maintenir) — acceptée car c'est l'objet même du projet.
