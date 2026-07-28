---
name: verify
description: Build/launch/drive recipe for this repo when Docker is unavailable
---

# Verify recipe (no-Docker fallback)

This repo's primary run path is `docker compose up --build` (see root `README.md`). When Docker
isn't available, a full local run (real Kafka included, no containers) is still possible and has
been done successfully — see below. This exercises the real ASP.NET Core apps end-to-end, not unit
tests.

## Setup (once per environment)

**.NET 8 SDK** (no sudo needed, installs to `$HOME/.dotnet`, ~220MB):
```bash
curl -sSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh && chmod +x /tmp/dotnet-install.sh
/tmp/dotnet-install.sh --channel 8.0 --install-dir "$HOME/.dotnet"
```
Check for a system-wide `dotnet` first (`which dotnet`) — if present it may be a newer major version
(e.g. .NET 10) that can *build* net8.0 projects fine but can't *run* them (missing net8.0 runtime →
`Win32Exception`/`framework not found`). Use `$HOME/.dotnet/dotnet` explicitly for `run`, not
whatever `dotnet` resolves to in PATH, unless you've confirmed it has the 8.0 runtime
(`dotnet --list-runtimes`).

**JDK** (only needed for a real Kafka broker — check `~/.jdks/*` first, IDEs often cache one already):
```bash
find ~/.jdks -maxdepth 1 -iname 'java' -o -iname 'corretto*' -o -iname 'openjdk*' 2>/dev/null
```
Any JDK 17+ works (Kafka 4.x requires 17+; a cached JDK 25 worked fine).

**Kafka** (binary, no Docker — use the fast mirror, not `archive.apache.org` which throttles hard,
~KB/s vs MB/s):
```bash
mkdir -p ~/kafka-local && cd ~/kafka-local
curl -sSL -o kafka.tgz "https://downloads.apache.org/kafka/4.1.2/kafka_2.13-4.1.2.tgz"  # check
curl -sSL "https://downloads.apache.org/kafka/" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | sort -uV | tail -1  # for current version
tar xzf kafka.tgz
export KAFKA_HOME="$HOME/kafka-local/kafka_2.13-4.1.2" JAVA_HOME=~/.jdks/<whatever> PATH="$JAVA_HOME/bin:$PATH"
CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
"$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_HOME/config/server.properties" --standalone
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties" > /tmp/kafka-broker.log 2>&1 &
```
Kafka 4.x's `config/server.properties` is already KRaft-ready out of the box (`process.roles=broker,controller`,
listeners on 9092/9093) — no need to hunt for a separate `config/kraft/` template like on 3.x.

## Build

```bash
D="$HOME/.dotnet/dotnet"
for d in RestaurantService CatalogService PaymentService DeliveryService OrderService NotificationService ApiGateway; do
  (cd "src/$d" && "$D" build --nologo)
done
```

## Launch a service standalone

```bash
cd src/RestaurantService
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 ASPNETCORE_URLS=http://127.0.0.1:8081 "$HOME/.dotnet/dotnet" run --no-build &
```
Port scheme matching the README/docker-compose mapping: gateway 8080, restaurant 8081, catalog 8082,
order 8083, payment 8084, delivery 8085, notification 8086. `order-service` additionally needs
`RESTAURANT_SERVICE_URL` pointed at restaurant-service's URL.

**API Gateway without Docker**: `ocelot.json` routes to Docker Compose service DNS names
(`restaurant-service:8080` etc.) which don't resolve standalone. Swap in a `127.0.0.1:<port>`
variant before building/running (back up and restore the original after — it's git-tracked):
```bash
cp ocelot.json ocelot.json.orig-backup   # then overwrite ocelot.json with 127.0.0.1:<port> routes, build, run
# afterward: cp ocelot.json.orig-backup ocelot.json && rm ocelot.json.orig-backup
```

## ⚠️ Kafka cold-start gotcha (cost real debugging time — read this)

If a consumer subscribes to a topic **before that topic exists**, and the topic only gets created
later by some other service's producer (normal here — no topics are pre-provisioned, they're all
auto-created on first use), the consumer can permanently miss it: `Consume()` throws one
`ConsumeException: Unknown topic or partition`, gets logged, the loop continues — but the client can
end up never (re)joining the consumer group for that specific topic, even though the topic now
exists and `kafka-consumer-groups.sh --describe` may not even show a row for it. This bit both
`order-service` (subscribes to `payment.events` + `delivery.events`) and `notification-service`
(subscribes to `order.events` + `payment.events` + `delivery.events`) on a completely fresh cluster:
they silently ended up only consuming the *first*-existing topic of their subscription list and
never the others — no crash, no repeated errors, just permanent silent lag on those topics forever
(confirmed via `kafka-consumer-groups.sh --describe --group <name>`, which showed no row at all for
the missing topic).

**Fix that works**: once all 6 topics exist (they will, after the first order has flowed through
once), **restart** `order-service` and `notification-service` (or just restart everything once,
after the first order's worth of activity, before doing real testing). A restart re-subscribes
against a broker where every topic already exists, and that always works cleanly. Alternative:
pre-create all 6 topics before starting any service:
```bash
for t in restaurant.events payment.commands payment.events delivery.commands delivery.events order.events; do
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --topic "$t" --partitions 3 --replication-factor 1
done
```
This is purely a cold-start-ordering artifact of a from-scratch broker with no pre-provisioned
topics; it is not a bug in the multi-topic subscription code itself, and does not affect
`docker compose` runs any differently in principle — same risk exists there too on a truly first-ever
`docker compose up`, mitigated in practice by Kafka's healthcheck plus the natural few seconds of
startup jitter between services, but worth knowing if a demo run behaves oddly on the very first
order.

## Other things learned running it for real

- **State is in-memory and per-process** (by design, ADR-0007): restarting any service wipes its
  orders/payments/deliveries. If you restart `order-service` mid-demo, previously created orders
  become `404 Commande introuvable` even though `notification-service`'s log of past events (itself
  stateless) still shows their full history. Don't restart services mid-demo unless starting fresh.
- **Couriers are a finite pool of 3** (2 available, 1 offline in seed data), and never freed until
  `POST /v1/delivery/deliveries/{id}/confirm` is called. Run 3 orders back-to-back without
  confirming any delivery and the 3rd will *genuinely* (not simulated) compensate via
  `NoCourierAvailable` → refund → `CANCELLED` — a good organic demo of the compensation path, but
  surprising if you're trying to test the happy path repeatedly. Confirm deliveries
  (`GET /v1/delivery/deliveries/order/{orderId}` for the id, then `POST .../confirm`) to free
  couriers back to `AVAILABLE` between test runs.
- The full happy path end-to-end timing: PAID/AWAITING_COURIER/CONFIRMED/IN_PREPARATION are near-
  instant; `IN_DELIVERY` (PICKED_UP) after ~5s, IN_TRANSIT after ~10s (both simulated delays in
  `DeliveryCommandsConsumer.SimulateProgressAsync`), `DELIVERED` only on explicit `/confirm`.
- `xdg-open "http://127.0.0.1:<port>/swagger/index.html"` (with `DISPLAY=:0` if not already set)
  pops a real browser window when a graphical session exists (`echo $DISPLAY`, `which firefox
  chromium`) — useful for handing the user a live Swagger UI instead of just curl output.
