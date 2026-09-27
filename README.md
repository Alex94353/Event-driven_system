# Event-driven replication demo

Three FastAPI nodes (`node_a`, `node_b`, `node_c`) each write to an independent MySQL database. A shared RabbitMQ topic exchange routes each event to a durable queue for every other node. A business write and its outbox event are committed in one database transaction; a receiver commits the order and inbox event together before acknowledging the message.

Orders use globally unique UUIDs. `origin_node` identifies the node that created a record, while `updated_by_node` breaks ties between concurrent writes with the same version. The deterministic winner is the lexicographically greater `(version, updated_by_node, event_id)` tuple. Deletions are retained as tombstones so stale updates cannot resurrect records. The outbox is an append-only event log: regular sync publishes unsent events, while `replay_all=true` replays the full log for recovery. Workers replay the local event log automatically after reconnecting.

## Start

```sh
cp .env.example .env
# Replace every placeholder in .env with a private value.
docker compose up --build --force-recreate -d
docker compose ps
```

Use `--build --force-recreate` after changing the source so all three API nodes and background services run the same image version. Check the API contract after deployment:

```sh
curl -fsS http://localhost:8000/openapi.json | python3 -c 'import json,sys; spec=json.load(sys.stdin); print(spec["paths"]["/orders/{record_id}"].keys())'
```

The API nodes are available at `http://localhost:8000`, `http://localhost:8001`, and `http://localhost:8002`. RabbitMQ management is at `http://localhost:15672`. Compose stores database and broker state in named volumes; `docker compose down` preserves them. Do not use `docker compose down -v` when demonstrating persistence.

Mutating requests, reads, manifests, status, and sync require the `X-API-Key` header set to the `API_KEY` value in `.env`. Health remains public for container healthchecks.

```sh
curl http://localhost:8000/health
curl -H "X-API-Key: $(grep '^API_KEY=' .env | cut -d= -f2-)" \
  -H 'Content-Type: application/json' \
  -d '{"item_name":"book","price":12}' \
  http://localhost:8000/orders/
```

Useful endpoints:

- `GET /health`: local database health; it remains healthy while a node is isolated from RabbitMQ.
- `GET /status`: local order, pending outbox, oldest pending event ID, and processed inbox counts.
- `GET /orders/`: visible records; add `?include_deleted=true` to include tombstones.
- `GET /manifest`: sorted records and SHA-256 checksum for comparison across nodes.
- `POST /sync`: publish unsent outbox events. Add `?replay_all=true` to replay all retained events.
- `POST /orders/`, `PUT /orders/{record_id}`, `DELETE /orders/{record_id}`: authenticated UUID-based CRUD.

## Defense scenario

1. Start all nodes and wait until `docker compose ps` reports healthy APIs, databases, and broker.
2. Create a record on A; poll authenticated `GET /manifest` on B and C until the UUID and checksum reflect it.
3. Isolate B's API, worker, and receiver from the shared broker network without stopping its API or database:

   ```sh
   for service in api_b worker_b receiver_b; do
     docker network disconnect event-system-broker "$(docker compose ps -q "$service")"
   done
   ```

   On Docker Desktop, detaching the network can invalidate the host port-forward for B. Use `docker compose exec -T api_b ...` for B requests while it is isolated; the API and database remain available inside the container.

4. Create one record on A and another on B. A's worker can route to B's durable queue; B's local write remains in B's outbox because its worker cannot reach RabbitMQ:

     ```sh
     docker compose exec -T api_b python -c 'import json,os,urllib.request; data=json.dumps({"item_name":"partition-B","price":42}).encode(); req=urllib.request.Request("http://127.0.0.1:8000/orders/", data=data, headers={"X-API-Key":os.environ["API_KEY"],"Content-Type":"application/json"}, method="POST"); print(urllib.request.urlopen(req).read().decode())'
     ```

5. Call sync inside B. It must return HTTP 503 while retaining B's unsent outbox event. Confirm `/status` reports a pending event:

     ```sh
     docker compose exec -T api_b python -c 'import os,urllib.request,urllib.error; req=urllib.request.Request("http://127.0.0.1:8000/sync", data=b"", headers={"X-API-Key":os.environ["API_KEY"]}, method="POST");
     try:
       urllib.request.urlopen(req)
     except urllib.error.HTTPError as error:
       print(error.code, error.read().decode())
       assert error.code == 503
     else:
       raise AssertionError("sync should fail while B is isolated")'
     docker compose exec -T api_b python -c 'import os,urllib.request; req=urllib.request.Request("http://127.0.0.1:8000/status",headers={"X-API-Key":os.environ["API_KEY"]}); print(urllib.request.urlopen(req).read().decode())'
     ```
6. Reconnect B's services:

   ```sh
   for service in api_b worker_b receiver_b; do
     docker network connect event-system-broker "$(docker compose ps -q "$service")"
   done
   ```

7. Wait for B's worker to publish and all receivers to catch up. Compare `count`, `records`, and `checksum` from `/manifest` on A, B, and C. If B's host port-forward remains unavailable after reconnect, read its manifest with `docker compose exec -T api_b python -c 'import os,urllib.request; req=urllib.request.Request("http://127.0.0.1:8000/manifest",headers={"X-API-Key":os.environ["API_KEY"]}); print(urllib.request.urlopen(req).read().decode())'`.
8. Repeat `POST /sync?replay_all=true` on each node. Inbox deduplication must keep counts and checksums unchanged.
9. Restart one database and its node services without deleting volumes, then verify `/manifest` and `/status` recover unchanged.

Malformed messages are routed to each node's durable `.events.dead` queue. Transient processing errors are retried through a durable quorum TTL queue with `MESSAGE_RETRY_DELAY_MS` delay, up to `MESSAGE_MAX_RETRIES` retries after the initial attempt, then routed to the same dead-letter queue. The retry queue uses RabbitMQ at-least-once dead lettering. If retry/dead-letter publication itself fails, the original broker delivery is requeued so it is not lost. Worker/receiver healthchecks require the process and both MySQL/RabbitMQ TCP endpoints to be available.

To compare node manifests manually, fetch `/manifest` from ports 8000, 8001, and 8002 with the `X-API-Key` header. The checksum is calculated from canonical JSON containing UUID, origin, last writer, last event ID, version, tombstone, and order data.

## Tests

```sh
docker build -t event-driven-system-test .
docker run --rm --entrypoint python event-driven-system-test -m pytest -q
```

The unit tests cover outbox retry and replay, receiver retry/ack behavior, duplicate delivery, out-of-order events, deterministic concurrent-version resolution, and delete tombstones. The defense procedure above is the Compose integration check for network isolation, automatic recovery, manifests, and persisted volumes.