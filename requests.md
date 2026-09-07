# Requests de teste

Pré-requisito: stack local no ar.

```bash
docker compose up -d --build
curl -sf http://localhost:8080/readyz
```

Credenciais do compose (senha em claro = `s3cr3t`):

| project_id | senha   |
|------------|---------|
| `billing`  | `s3cr3t` |
| `checkout` | `s3cr3t` |

Base URL: `http://localhost:8080`

---

## Health

```bash
curl -i http://localhost:8080/healthz
curl -i http://localhost:8080/readyz
```

Esperado: `200` com `{"status":"ok"}`.

---

## Ingestão feliz

```bash
curl -i -u 'billing:s3cr3t' \
  -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-order-1' \
  -d '{
    "action": "order.created",
    "actor": {"type": "user", "id": "usr_123"},
    "resource": {"type": "order", "id": "ord_456"},
    "outcome": "success",
    "occurred_at": "2026-09-07T12:00:00Z",
    "metadata": {
      "amount_cents": 1500,
      "currency": "BRL",
      "plan_id": "pro",
      "actor_email": "user@example.com",
      "campaign": "spring"
    }
  }'
```

Esperado: `202 Accepted`

```json
{"id":"<uuid>","received_at":"<rfc3339>"}
```

`amount_cents` / `currency` / `plan_id` / `actor_email` vão para `metadata`
(allowlist). `campaign` vai para `extensions.metadata`.

---

## Idempotência com header

Repita o mesmo request. Deve devolver `202` com o **mesmo** `id`.

```bash
curl -s -u 'billing:s3cr3t' \
  -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-order-1' \
  -d '{
    "action": "order.created",
    "actor": {"type": "user", "id": "usr_123"},
    "resource": {"type": "order", "id": "ord_456"},
    "outcome": "success",
    "occurred_at": "2026-09-07T12:00:00Z",
    "metadata": {
      "amount_cents": 1500,
      "currency": "BRL",
      "plan_id": "pro",
      "actor_email": "user@example.com",
      "campaign": "spring"
    }
  }'
```

---

## Idempotência sem header

Sem `Idempotency-Key`, a chave é derivada do body canônico. Dois POSTs
idênticos devem devolver o mesmo `id`.

```bash
BODY='{
  "action": "invoice.paid",
  "actor": {"type": "service", "id": "billing"},
  "resource": {"type": "invoice", "id": "inv_1"},
  "outcome": "success",
  "occurred_at": "2026-09-07T13:00:00Z"
}'

curl -s -u 'billing:s3cr3t' -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' -d "$BODY"

curl -s -u 'billing:s3cr3t' -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' -d "$BODY"
```

---

## Redação de segredos

O body abaixo contém um token. Ele é mascarado **antes** do INSERT; a API
ainda responde `202`.

```bash
curl -i -u 'checkout:s3cr3t' \
  -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "action": "payment.authorized",
    "actor": {"type": "user", "id": "usr_9"},
    "resource": {"type": "payment", "id": "pay_1"},
    "outcome": "success",
    "occurred_at": "2026-09-07T14:00:00Z",
    "metadata": {
      "amount_cents": 4200,
      "currency": "BRL",
      "api_key": "must_be_redacted_by_key_name"
    }
  }'
```

Conferir no Postgres:

```bash
docker compose exec postgres \
  psql -U audit -d audit -c \
  "SELECT action, metadata, extensions FROM events ORDER BY received_at DESC LIMIT 3;"
```

`api_key` deve aparecer como `***REDACTED***`.

---

## Erros

### Sem autenticação → `401`

```bash
curl -i -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"action":"x"}'
```

### Credencial inválida → `401`

```bash
curl -i -u 'billing:wrong' \
  -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"action":"x"}'
```

### Body inválido → `400`

```bash
curl -i -u 'billing:s3cr3t' \
  -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"action":"order.created"}'
```

Esperado: JSON com `code` de validação (campo obrigatório ausente).

### `occurred_at` muito antigo → `400`

Eventos mais velhos que `HOT_RETENTION` são rejeitados (cairiam em partição
já arquivável).

```bash
curl -i -u 'billing:s3cr3t' \
  -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "action": "order.created",
    "actor": {"type": "user", "id": "usr_1"},
    "resource": {"type": "order", "id": "ord_old"},
    "outcome": "success",
    "occurred_at": "2020-01-01T00:00:00Z"
  }'
```

### Rate limit → `429`

No compose o limite default é alto (`100` rps / burst `200`). Para forçar:

```bash
RATE_LIMIT_RPS=1 RATE_LIMIT_BURST=1 docker compose up -d api

for i in 1 2 3; do
  curl -s -o /dev/null -w "%{http_code}\n" -u 'billing:s3cr3t' \
    -X POST http://localhost:8080/v1/events \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: rate-$i" \
    -d "{
      \"action\": \"ping\",
      \"actor\": {\"type\": \"service\", \"id\": \"cli\"},
      \"resource\": {\"type\": \"probe\", \"id\": \"$i\"},
      \"outcome\": \"success\",
      \"occurred_at\": \"2026-09-07T15:00:0${i}Z\"
    }"
done

# restaura
docker compose up -d api
```

---

## Arquivamento (`migrate`)

O compose usa `HOT_RETENTION=1h` no job, mas a unidade de arquivamento ainda é
a **partição semanal fechada**. Eventos da semana ISO atual não saem. Para
exercitar o cold path:

1. Ingerir eventos de uma semana ISO anterior (e com `occurred_at` dentro de
   `HOT_RETENTION` da API — default 90d):

```bash
# Segunda-feira da semana ISO anterior (ajuste se necessário)
PREV_WEEK=$(python3 - <<'PY'
from datetime import date, timedelta
today = date.today()
monday = today - timedelta(days=today.weekday())
prev = monday - timedelta(days=7)
print(prev.isoformat() + "T12:00:00Z")
PY
)

curl -i -u 'billing:s3cr3t' \
  -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: archive-seed-1' \
  -d "{
    \"action\": \"order.archived_seed\",
    \"actor\": {\"type\": \"user\", \"id\": \"usr_arch\"},
    \"resource\": {\"type\": \"order\", \"id\": \"ord_arch\"},
    \"outcome\": \"success\",
    \"occurred_at\": \"${PREV_WEEK}\",
    \"metadata\": {\"amount_cents\": 100, \"currency\": \"BRL\"}
  }"
```

2. Rodar o job (profile `migrate`):

```bash
docker compose --profile migrate run --rm migrate
```

3. Conferir objetos no MinIO e estado no Postgres:

```bash
docker compose run --rm --entrypoint sh minio-init -c \
  'mc alias set local http://minio:9000 minioadmin minioadmin && mc ls -r local/audit-cold'

docker compose exec postgres \
  psql -U audit -d audit -c \
  'SELECT partition_name, status, rows_exported FROM cold_archive_jobs ORDER BY started_at DESC LIMIT 5;'
```

Console MinIO: http://localhost:9001 (`minioadmin` / `minioadmin`).

---

## Métricas

Com o collector no ar, a API exporta OTLP a cada 10s:

```bash
docker compose logs -f otel-collector
```

Depois de alguns POSTs bem-sucedidos, procure por
`http.server.request.total` / `http.server.request.duration`.

---

## Derrubar

```bash
docker compose down -v
```
