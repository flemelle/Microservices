---
name: verify
description: Build/launch/drive recipe for this repo when Docker is unavailable
---

# Verify recipe (no-Docker fallback)

This repo's primary run path is `docker compose up --build` (see root `README.md`). When Docker
isn't available in the environment, use this instead — it exercises the real ASP.NET Core apps,
not unit tests.

## Setup (once per environment)

```bash
curl -sSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh && chmod +x /tmp/dotnet-install.sh
/tmp/dotnet-install.sh --channel 8.0 --install-dir "$HOME/.dotnet"
export DOTNET_ROOT="$HOME/.dotnet" PATH="$HOME/.dotnet:$PATH" DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1
```

No sudo needed. Installs to `$HOME/.dotnet`, ~220MB download.

## Build

```bash
for d in RestaurantService CatalogService PaymentService DeliveryService OrderService NotificationService ApiGateway; do
  (cd "src/$d" && dotnet build --nologo)
done
```

## Launch a service standalone

Each service reads `KAFKA_BOOTSTRAP_SERVERS` and binds `ASPNETCORE_URLS`. Kafka-dependent code
paths (event publish, background consumers) degrade gracefully if unreachable — safe to launch
without Kafka running:

```bash
cd src/RestaurantService
KAFKA_BOOTSTRAP_SERVERS=localhost:19999 ASPNETCORE_URLS=http://127.0.0.1:5082 dotnet run --no-build &
```

`order-service` additionally needs `RESTAURANT_SERVICE_URL` pointed at a running
`restaurant-service` to exercise the resilient client (Circuit Breaker/Retry/Timeout).

## What's drivable without Kafka

- `restaurant-service`: full CRUD, `/validate`, `/v1/_chaos/enable|disable` (simulates 500s to
  trigger the Circuit Breaker from `order-service`).
- `payment-service` / `delivery-service`: the synchronous debug REST endpoints only (not the
  Kafka command/event flow).
- `order-service`: `POST /v1/orders` still succeeds (201) end-to-end even with Kafka down —
  Kafka publish failures are caught and logged, never fail the HTTP response — but each of the
  two sequential awaited publishes (`OrderCreated`, `ProcessPayment`) blocks for up to
  `MessageTimeoutMs` (5000ms) if the broker is unreachable, so expect ~10s+ latency on
  `POST /v1/orders` in that state. This does not happen in normal `docker compose` operation
  (Kafka's healthcheck gates service startup), but is worth knowing if probing manually.

## What needs real Kafka (Docker required)

The full SAGA (`payment.events`/`delivery.events` consumption inside `order-service`), the
`catalog-service` CQRS projection, and `notification-service` all require a live broker — not
verifiable via the standalone launch above. Use `docker compose up --build` and the scenarios in
`README.md` for that.

## Gotchas

- `dotnet run --no-build` fails with a confusing `Win32Exception ... No such file or directory`
  if `bin/`/`obj/` were cleaned since the last build — rebuild first.
- Background `dotnet run` processes bind real ports; `kill $PID` and `wait` between runs to avoid
  "address already in use" on retry.
