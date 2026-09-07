# audit

Serviço de auditoria HTTP **write-only**: outros serviços enviam eventos, que
ficam no PostgreSQL na janela quente e são arquivados em Parquet no object
storage ao esfriar. Leitura e consulta de eventos são responsabilidade de outro
sistema.

Implementa a [spec 001](docs/specs/001-audit-service.md). Contrato da API em
[docs/openapi.yaml](docs/openapi.yaml).

## Como funciona

```
POST /v1/events  ──Basic Auth──▶  valida ──▶ mascara segredos ──▶ INSERT síncrono
                                                                         │
                                                              PostgreSQL (90 dias,
                                                              partições semanais)
                                                                         │
                                            audit migrate (diário) ──────┘
                                                     │
                                       Parquet + zstd, criptografado
                                       (S3 SSE-KMS ou Azure CMK)
```

O `202` só volta depois do commit no PostgreSQL: o evento já é durável quando o
cliente recebe a resposta.

## Modos de execução

Um binário, uma imagem, dois modos escolhidos por argumento:

```bash
audit api        # servidor HTTP de ingestão (long-running)
audit migrate    # uma passada do arquivamento quente → frio, e encerra
```

```bash
docker run --rm -p 8080:8080 audit:latest api
docker run --rm audit:latest migrate
```

Sem argumento ou com um argumento desconhecido, o binário imprime o help em
stderr e sai com código 2.

## Ingestão

O `username` do Basic Auth é o `project_id`. A identidade vem sempre da
autenticação: `project_id`, `id`, `received_at` e `schema_version` no body são
descartados.

```bash
curl -u billing:s3cr3t -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -d '{
    "action": "order.created",
    "actor": {"type": "user", "id": "usr_123"},
    "resource": {"type": "order", "id": "ord_456"},
    "outcome": "success",
    "occurred_at": "2026-09-07T12:00:00Z",
    "metadata": {"amount_cents": 1500, "currency": "BRL"}
  }'
```

```json
{"id":"0f5c5c79-a7bb-42ca-ba79-78dcc19ca56e","received_at":"2026-09-07T12:00:00.775Z"}
```

**Idempotência.** Com o header `Idempotency-Key`, ele é usado com escopo no
projeto. Sem ele, a chave é derivada de
`sha256(project_id + "\n" + canonical_json(body))`. Nos dois casos, um retry
devolve `202` com o `id` do evento original em vez de duplicar.

**Onde cada campo vai.** Os campos conhecidos viram colunas tipadas. Chaves de
`metadata` que estão na allowlist global vão para a coluna `metadata`; as demais
são preservadas em `extensions.metadata`. Qualquer outro campo do body vai para
`extensions`.

**Máscara de segredos.** Antes do INSERT, tokens, chaves de API, JWTs, blocos
PEM e credenciais em URL são substituídos por `***REDACTED***`, tanto por nome
de campo quanto por formato do valor. O log registra só a contagem
(`fields_redacted_count`), nunca o valor original.

## Configuração

Tudo vem do ambiente. Cada modo valida no boot apenas o que precisa.

### Comum

| Variável | Default | Descrição |
|----------|---------|-----------|
| `DATABASE_URL` | — | Obrigatória nos dois modos |
| `AUDIT_ENV` | `development` | Compõe o prefixo do object storage |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `HOT_RETENTION` | `2160h` (90d) | Janela quente; também rejeita eventos mais antigos |
| `DB_AUTO_MIGRATE` | `true` | Aplica as migrations no boot |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Sem ela, as métricas ficam desligadas |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | `grpc` ou `http` |
| `OTEL_EXPORTER_OTLP_HEADERS` | — | Ex.: `Authorization=ApiKey ...` |

### Modo `api`

| Variável | Default | Descrição |
|----------|---------|-----------|
| `HTTP_ADDR` | `:8080` | Endereço de escuta |
| `AUTH_CREDENTIALS_SOURCE` | `env` | `env` ou `db` (tabela `services`) |
| `AUDIT_CREDENTIALS` | — | `project_id:bcrypt_hash,...` quando a fonte é `env` |
| `MAX_BODY_BYTES` | `65536` | Teto do body (64 KiB) |
| `METADATA_MAX_BYTES` / `METADATA_MAX_KEYS` | `4096` / `20` | Limites de `metadata` |
| `METADATA_ALLOWLIST` | `amount_cents,currency,plan_id,actor_email` | Allowlist global |
| `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` | `100` / `200` | Por `project_id`; `0` desliga |
| `MAX_CLOCK_SKEW` | `24h` | Quanto `occurred_at` pode estar no futuro |

O segredo é sempre um hash bcrypt — o valor em claro nunca é aceito nem
armazenado. Como bcrypt custa dezenas de milissegundos, o resultado da
verificação fica em cache (`AUTH_CACHE_TTL`, default `5m`) para não consumir o
orçamento de latência.

### Modo `migrate`

| Variável | Default | Descrição |
|----------|---------|-----------|
| `COLD_STORAGE_PROVIDER` | `s3` | `s3` ou `azure` |
| `COLD_STORAGE_BUCKET` | — | Bucket S3 ou container Azure |
| `COLD_STORAGE_PREFIX` | `audit` | Prefixo raiz das keys |
| `COLD_STORAGE_KMS_KEY_ID` | — | CMK para SSE-KMS; obrigatória no `s3` |
| `COLD_STORAGE_AZURE_ACCOUNT_URL` | — | Obrigatória no `azure` |
| `COLD_STORAGE_AZURE_ENCRYPTION_SCOPE` | — | Encryption scope com CMK; obrigatória no `azure` |
| `ARCHIVE_BATCH_ROWS` | `200000` | Linhas por arquivo Parquet |
| `ARCHIVE_MAX_PARTITIONS_PER_RUN` | `8` | Teto de partições por execução |
| `ARCHIVE_DRY_RUN` | `false` | Exporta e valida, mas não faz DROP |

O job é **idempotente**: a unidade de progresso é o lote, as keys são
determinísticas e o estado fica em `cold_archive_jobs`/`cold_archive_batches`.
Uma execução interrompida é retomada pela próxima, que reprocessa só o que
faltou. A partição só sai do PostgreSQL depois que todos os lotes estão
verificados e a soma das linhas bate com o `COUNT(*)`.

Layout no object storage:

```
{bucket}/audit/{env}/project_id={p}/year={YYYY}/week={WW}/
  events-{partition}-{seq:05d}.parquet
  events-{partition}-{seq:05d}.manifest.json
```

## Observabilidade

Logs em JSON via `log/slog` em stdout, sempre com `request_id`,
`correlation_id` e `project_id`. Métricas via OpenTelemetry exportadas por OTLP
(RPM e latência p50/p99 da ingestão; duração, linhas, bytes e falhas por motivo
no cold path). No modo `migrate` o MeterProvider é drenado antes de o processo
sair, para não perder o último export.

## Desenvolvimento

### Stack local

```bash
docker compose up -d --build
```

Sobe PostgreSQL, MinIO (SSE-KMS), collector OTLP e a API em `:8080`. Credenciais
de exemplo: `billing` / `checkout` com senha `s3cr3t`. Exemplos de request em
[requests.md](requests.md).

```bash
docker compose --profile migrate run --rm migrate   # uma passada do cold path
docker compose down -v                              # derruba e apaga volumes
```

### Testes

```bash
go vet ./...
go test ./... -race
```

Os testes de `internal/store/postgres` exercitam o SQL de verdade
(particionamento semanal, `jsonb`/`inet`, idempotência, `DETACH`/`DROP`). Eles
são pulados sem um banco disponível — o Postgres do compose serve:

```bash
AUDIT_TEST_DATABASE_URL='postgres://audit:audit@localhost:5432/audit?sslmode=disable' \
  go test ./... -race
```

Para gerar um hash de credencial:

```bash
htpasswd -bnBC 10 "" 's3cr3t' | tr -d ':\n'
```

## Deploy

`pipelines/pipe.yml` roda testes, race detector, build e push para o ACR, e
publica a **mesma imagem** em dois lugares: um Azure Container App com
`args: api` e um Container Apps Job agendado com `args: migrate`.
