# Spec 001 — Serviço de Auditoria HTTP

**Status:** draft  
**Data:** 2026-09-07  
**Escopo:** ingestão de eventos de auditoria via HTTP (write-only) + job de migração para frio; autenticação por `project_id`.

---

## 1. Objetivo

Prover um serviço HTTP centralizado onde outros serviços enviam eventos de auditoria. Cada remetente se autentica com Basic Auth; o username é o `project_id` de origem. Os eventos ficam no PostgreSQL na janela quente e são arquivados para S3 (Parquet) ao esfriar. A **leitura/consulta** fica a cargo de outro sistema.

---

## 2. Requisitos

### Funcionais

- Ingestão de eventos via um único endpoint HTTP (`POST`), payload inteiro no body.
- Autenticação Basic Auth; o `username` é o `project_id` do remetente.
- Persistência de eventos com metadados suficientes para rastreabilidade.
- Identidade do remetente derivada da autenticação, não do body (body não pode forjar `project_id`).
- Idempotência na ingestão: header `Idempotency-Key` ou chave derivada automaticamente (ver 4.1 / decisões).

### Não-funcionais

- Reduzir espaço de armazenamento (volume alto, retenção longa).
- **Latência de escrita (ingestão):** `POST /v1/events` deve responder em **≤ 100 ms** (p99), medido no servidor (fim do handler → status de sucesso), excluindo RTT de rede do cliente.
- **Observabilidade:** logs com **`log/slog`** (stdlib) e métricas com **OpenTelemetry** exportadas para **Elastic**; evidência objetiva dos SLOs (ver seção 11).
- **Empacotamento simples:** um `main.go`, um `Dockerfile`, modos `api` e `migrate` via args (ver seção 9).

### Fora de escopo (v1)

- **Leitura/consulta de eventos** (quente ou frio): outro sistema será responsável; este serviço é **write-only** (+ job `migrate`).
- UI de visualização.
- Stream em tempo real (WebSocket/SSE).
- SQL livre / endpoint admin de query.
- GDPR purge seletivo (definir em spec seguinte).

---

## 3. Autenticação

### Basic Auth

```
Authorization: Basic base64(project_id:secret)
```

| Campo | Uso |
|-------|-----|
| `username` | `project_id` estável do projeto/remetente, ex.: `billing`, `checkout` |
| `password` | Segredo por serviço (hash no servidor; nunca logar em claro) |

### Comportamento

1. Extrair credenciais do header.
2. Validar contra store de credenciais (env, vault ou tabela `services`).
3. Em sucesso, anexar `project_id` ao contexto da request.
4. Em falha: `401 Unauthorized` sem detalhes.

### Credenciais

- Cadastro inicial via config/migration (v1).
- Rotação de secret sem downtime (suportar dois secrets ativos por serviço, opcional v1.1).

---

## 4. Endpoints

### 4.1 Ingestão — `POST /v1/events`

Recebe **todo** o conteúdo do evento no body (JSON).

**Headers**

- `Authorization: Basic ...` (obrigatório)
- `Content-Type: application/json`
- `Idempotency-Key` (opcional): se ausente, o servidor **deriva** a chave (ver abaixo)

**Body (contrato lógico)**

```json
{
  "action": "order.created",
  "actor": {
    "type": "user",
    "id": "usr_123"
  },
  "resource": {
    "type": "order",
    "id": "ord_456"
  },
  "outcome": "success",
  "occurred_at": "2026-09-07T12:00:00Z",
  "request_id": "req_abc",
  "correlation_id": "corr_xyz",
  "ip": "203.0.113.10",
  "user_agent": "svc-billing/1.2.0",
  "metadata": {
    "amount_cents": 1500,
    "currency": "BRL"
  }
}
```

**Campos obrigatórios (v1):** `action`, `occurred_at`  
**Campos recomendados:** `actor`, `resource`, `outcome`, `request_id`  
**Derivados pelo servidor:** `id`, `project_id`, `received_at`, `schema_version`


**Idempotência**

1. Se o cliente enviar `Idempotency-Key`, usar esse valor (escopo = `project_id`).
2. Se **não** enviar: gerar chave determinística a partir de `project_id` + representação canônica do body (campos de negócio + `metadata` / payload relevante), ex.:  
   `idempotency_key = hex(sha256(project_id + "\n" + canonical_json(body)))`.
3. Persistência com `UNIQUE (project_id, idempotency_key)`: retry com o mesmo body (ou mesma key) não duplica evento → `409` ou `202` com o `id` já existente (escolher uma semântica e documentar na OpenAPI; preferência: **`202` + id original** para retries seguros).

**Respostas**

| Status | Quando |
|--------|--------|
| `202 Accepted` | Evento aceito (persistência confirmada ou enfileirada) |
| `400` | Body inválido / campos obrigatórios ausentes |
| `401` | Auth inválida |
| `409` | Conflito de idempotência (se a API optar por 409 em vez de reapresentar o id) |
| `413` | Body acima do limite |
| `429` | Rate limit por `project_id` |

**Resposta de sucesso (exemplo)**

```json
{
  "id": "evt_01H...",
  "received_at": "2026-09-07T12:00:00.123Z"
}
```


---

## 5. Onde salvar — híbrido (Postgres quente + S3 frio)

Decisão: **armazenamento híbrido**. Escrita no PostgreSQL (quente); dados que saem da janela quente vão para S3 e saem do banco. Leitura não faz parte deste serviço.

| Camada | Tecnologia | Papel |
|--------|------------|--------|
| **Quente** | PostgreSQL (particionado por `occurred_at`) | Ingestão ≤ 100 ms; fonte do job `migrate` |
| **Fria** | Object storage (**AWS S3** ou **Azure Blob**) | Retenção longa, custo baixo; leitura em outro sistema |

### 5.1 Por quê

1. SLO de escrita e durabilidade exigem um store transacional no hot path → Postgres (a leitura/filtro fica em outro sistema).
2. Manter anos de auditoria só em PG encarece disco, índices e vacuum.
3. Partições temporais permitem exportar e **DROP** blocos inteiros sem `DELETE` linha a linha.
4. Object storage guarda Parquet (criptografado em repouso) para retenção longa / consumo externo.

### 5.2 Janelas de retenção

| Camada | Janela (v1) | Comportamento |
|--------|-------------|---------------|
| Quente (PG) | **90 dias** a partir de `occurred_at` | Disponível para o job `migrate` exportar |
| Fria (S3 / Azure Blob) | **≥ 2 anos** (lifecycle do bucket/container) | Consumida por **outro sistema**; este serviço não expõe leitura |

Este serviço **não** oferece API de leitura sobre quente nem frio.

### 5.3 Layout no object storage (S3 ou Azure Blob)

Mesmo layout lógico nos dois provedores (`s3://` ou container Azure):

```text
{bucket_or_container}/audit/{env}/
  project_id={project_id}/
    year={YYYY}/
      week={WW}/                 # ISO week; alinhado à partição PG semanal
        events-{partition_name}-{batch_seq:05d}.parquet
        events-{partition_name}-{batch_seq:05d}.manifest.json
```

| Artefato | Conteúdo |
|----------|----------|
| `*.parquet` | Lotes em **Parquet + compressão zstd**, schema alinhado ao quente (seção 6) |
| `*.manifest.json` | `partition_name`, `project_id`, `occurred_at_min/max`, `row_count`, `sha256`, `parquet_schema_version`, `exported_at` |

Objetos imutáveis após upload (checksum no manifest). Prefixo por `project_id` + semana facilita lifecycle.

#### Criptografia em repouso (decisão 12.7)

| Provedor | Mecanismo | SDK |
|----------|-----------|-----|
| **AWS S3** | **SSE-KMS** (CMK) | AWS SDK for Go v2 (`s3` + `ServerSideEncryption: aws:kms`) |
| **Azure Blob** | Encryption at rest com **CMK** (Azure Key Vault) | Azure SDK for Go (`azblob`; encryption policy / scope com CMK) |

Abstração interna `BlobStore` (Put/Get/Head) com duas implementações; o job `migrate` não fala S3/Azure direto nos callers. Credenciais e key ids via env (`COLD_STORAGE_PROVIDER=s3|azure`, KMS/Key Vault ids, etc.).

### 5.4 Quando arquivar (gatilho)

Job agendado (**diário**, janela de baixo tráfego, ex.: 03:00 UTC):

1. Listar partições PG cujo **limite superior** (`occurred_at` < início do dia) seja **mais antigo que `hot_retention` (90 dias)**.
2. Ignorar a partição corrente e qualquer partição ainda dentro da janela quente.
3. Processar **uma partição por vez** (ou N com concurrency baixa) para não saturar I/O do PG.
4. Só arquivar partições **fechadas** (nenhum insert novo esperado naquele range — garantido pelo `PARTITION BY RANGE`).

Regra: `archive_if: partition.upper_bound <= now() - interval '90 days'`.

### 5.5 Como arquivar (fluxo) — idempotente

```text
[CronJob / scheduler → `audit migrate`]
    → SELECT partições elegíveis ainda não dropped
    → para cada partição:
        1. UPSERT cold_archive_jobs (partition_name); se status=dropped → skip
        2. status=exporting; job_id / run_id no log
        3. Descobrir lotes pendentes (não uploaded/verified) — ver idempotência
        4. Para cada lote pendente:
             stream PG → escrever .parquet (zstd) → upload object storage (key determinística, SSE-KMS/CMK)
             gravar cold_archive_batches (uploaded) + manifest parcial
        5. Validar: soma row_count dos batches verified == COUNT(*) da partição
           && checksums OK
        6. status=exported; só então DETACH + DROP PARTITION
        7. status=dropped; métricas do run
```

**Tabela de controle (PG)**

```text
cold_archive_jobs (
  id               uuid PK,
  partition_name   text NOT NULL UNIQUE,
  occurred_at_from timestamptz NOT NULL,
  occurred_at_to   timestamptz NOT NULL,
  status           text NOT NULL,  -- pending | exporting | exported | dropped | failed
  s3_prefix        text,
  row_count        bigint,         -- total esperado / validado
  bytes_total      bigint,
  error            text,           -- último motivo de falha (humano + código)
  error_code       text,           -- ex.: s3_upload, checksum_mismatch, pg_read
  started_at       timestamptz,
  finished_at      timestamptz,
  updated_at       timestamptz NOT NULL
)

cold_archive_batches (
  id               uuid PK,
  job_id           uuid NOT NULL REFERENCES cold_archive_jobs(id),
  batch_seq        int NOT NULL,   -- 0..N estável por partição
  status           text NOT NULL,  -- pending | uploaded | verified | failed
  s3_key           text NOT NULL,  -- determinística
  row_count        bigint,
  bytes            bigint,
  sha256           text,
  occurred_at_from timestamptz,    -- range do lote (cursor)
  occurred_at_to   timestamptz,
  error            text,
  error_code       text,
  UNIQUE (job_id, batch_seq),
  UNIQUE (s3_key)
)
```

**Idempotência (falha → retomar só o que falta)**

1. Unidade de progresso = **batch** dentro da partição (`batch_seq`), não a partição inteira.
2. Keys S3 determinísticas:  
   `.../events-{partition_name}-{batch_seq:05d}.parquet`  
   Reexecução não cria objeto “novo”; sobrescreve o mesmo key ou faz skip se `status=verified` e objeto existe.
3. Em retry do job:
   - Batches `verified` → **não** releem o PG nem reenviam.
   - Batches `uploaded` sem verify → só revalida checksum/manifest (ou re-upload se objeto ausente).
   - Batches `pending` / `failed` → reprocessa apenas esses.
4. Partição só vai para `exported`/`dropped` quando **todos** os batches estão `verified` e a soma bate com o `COUNT(*)` da partição.
5. Se o processo morrer no meio: próximo run diário (ou retry) continua do estado em `cold_archive_*`; não reinicia do zero.

**Garantias**

- **Exactly-once lógico no frio:** um `batch_seq` → um `s3_key`; DROP só após validação completa.
- **Falha:** `job.status=failed` + `error`/`error_code`; batches com falha guardam o motivo; partição **permanece** no PG.
- Log de falha obrigatório com: `job_id`, `partition_name`, `batch_seq`, `error_code`, `error`, `s3_key` (se houver).

**O que não fazer**

- Não arquivar por `DELETE` + vacuum linha a linha.
- Não escrever no S3 no hot path da ingestão (mantém p99 ≤ 100 ms).
- Não expor endpoints de leitura neste serviço.
- Não reexportar batches já `verified` em retries normais.

### 5.6 Interação com a API

| Operação | Store |
|----------|--------|
| `POST /v1/events` | somente PG (quente) |
| `migrate` (CLI) | lê PG → escreve Parquet no object storage (S3/Azure) → DROP partição |
| Leitura de eventos | **fora deste serviço** (outro sistema) |

### 5.7 Observabilidade do cold path

Detalhes unificados na **seção 11.2** (métricas de duração, volume diário, falhas e logs com motivo).

---

## 6. Formato de armazenamento

Decisão:

| Camada | Formato |
|--------|--------|
| **Quente (PG)** | Colunas tipadas + JSONB |
| **Fria (S3)** | Apache Parquet |
| **Particionamento** | `PARTITION BY RANGE (occurred_at)` **semanal** — unidade de export/DROP |

### 6.1 Quente — colunas tipadas + JSONB

```text
events (
  id              uuid PK,
  project_id      text NOT NULL,
  schema_version  smallint NOT NULL,
  action          text NOT NULL,
  outcome         text,
  actor_type      text,
  actor_id        text,
  resource_type   text,
  resource_id     text,
  request_id      text,
  correlation_id  text,
  ip              inet,
  user_agent      text,
  occurred_at     timestamptz NOT NULL,
  received_at     timestamptz NOT NULL,
  metadata        jsonb NOT NULL DEFAULT '{}',  -- allowlist global
  extensions      jsonb NOT NULL DEFAULT '{}'   -- resto do body
) PARTITION BY RANGE (occurred_at);
```

Índices: `(project_id, occurred_at DESC)`, `(project_id, action, occurred_at)`, opcional `GIN (metadata)`.

| Campo JSONB | Uso |
|-------------|-----|
| `metadata` | Chaves da **allowlist global** |
| `extensions` | Demais campos do body fora da allowlist |

**Allowlist de metadata — global (decisão)**

Uma lista única para todos os `project_id` (config/env), ex.:

```yaml
metadata_keys:
  - amount_cents   # int
  - currency       # text
  - plan_id        # text
  - actor_email    # text (se permitido globalmente)
```

Chaves fora da allowlist vão para `extensions` (persistidas, sem promoção a coluna filtrável neste serviço).

**Por quê JSONB no quente**

- Filtro SQL nativo (`metadata->>'currency'`, `@>`, etc.) sem codec extra no hot path.
- Ingestão simples (parse JSON → colunas + dois JSONB) — ajuda o p99 ≤ 100 ms.
- Espaço aceitável na janela de 90 dias; o ganho de compressão fica no frio (Parquet).

### 6.2 Frio — Parquet

No arquivamento, cada lote vira um arquivo Parquet cujo schema espelha a tabela quente:

| Coluna Parquet | Tipo lógico | Origem PG |
|----------------|-------------|-----------|
| `id` | UUID (string/fixed) | `id` |
| `project_id` | UTF8 | `project_id` |
| `schema_version` | INT16 | `schema_version` |
| `action` | UTF8 | `action` |
| `outcome` | UTF8 | `outcome` |
| `actor_type` / `actor_id` | UTF8 | idem |
| `resource_type` / `resource_id` | UTF8 | idem |
| `request_id` / `correlation_id` | UTF8 | idem |
| `ip` / `user_agent` | UTF8 | idem |
| `occurred_at` / `received_at` | TIMESTAMP (UTC) | idem |
| `metadata` | UTF8 (JSON) | `metadata::text` |
| `extensions` | UTF8 (JSON) | `extensions::text` |

**Convenções v1**

- Compressão de página/coluna: **zstd** (densidade/custo de storage acima de throughput/CPU do job).
- `metadata` e `extensions` serializados como **string JSON** no Parquet (round-trip simples; evita divergência de MAP tipado).
- Row groups dimensionados para os lotes do job (~64–128 MiB por arquivo).
- `parquet_schema_version` no manifest acompanha evolução do schema frio.

**Por quê Parquet no frio**

- Compressão colunar → menos bytes que JSON/JSONB repetido em retenção longa.
- Formato adequado para **outro sistema** de leitura/analytics consumir o bucket.
- Alinhado ao export por partição **semanal** sem acoplar o hot path a codec binário.

### 6.3 Resumo

| Camada | Formato | Motivo |
|--------|--------|--------|
| Quente | Colunas + `metadata`/`extensions` JSONB | SQL + ingestão rápida |
| Tempo | Partições **semanais** em `occurred_at` | Unidade de archive/DROP |
| Frio | Parquet **zstd** (+ manifest) em S3 ou Azure Blob | Densidade + retenção barata |
| Segredo em repouso | SSE-KMS (S3) / CMK Key Vault (Azure) | Decisão 12.7 |
| Segredo em trânsito/ingest | Máscara de tokens/chaves no body | Decisão 12.7 |
| Identidade | `project_id` da auth | Integridade do remetente |

### 6.4 Schema de versão

- `schema_version` (smallint) em cada evento (quente e frio).
- Evolução do body/API sem quebrar leitores; export Parquet registra `parquet_schema_version` no manifest.

---

## 7. Modelo lógico do evento (camada de API)

```text
Event
├── id                 (server)
├── project_id         (from Basic Auth)
├── schema_version     (server)
├── action
├── actor { type, id }
├── resource { type, id }
├── outcome
├── occurred_at
├── received_at        (server)
├── request_id
├── correlation_id
├── ip / user_agent    (opcional)
├── metadata           (allowlist global → jsonb)
└── extensions         (resto do body → jsonb)
```

O registro lógico é a composição colunas + `metadata` + `extensions` (consumidores externos / outro sistema de leitura).

---

## 8. Limites e políticas (proposta)

| Limite | Valor sugerido v1 |
|--------|-------------------|
| Body máximo | 64 KiB |
| `metadata` máximo | 4 KiB / 20 chaves |
| Rate limit | por `project_id` (ex.: 100 rps) |
| Retenção quente (PG) | 90 dias (`occurred_at`) |
| Retenção fria (S3/Azure) | ≥ 2 anos (lifecycle) |
| Arquivamento | Job diário; partição **semanal** com `upper_bound <= now() - 90d` |
| Compressão Parquet | **zstd** |
| Criptografia frio | SSE-KMS (AWS) / CMK (Azure) |
| Latência ingestão (p99) | ≤ 100 ms (server-side) |

---

## 9. Empacotamento e modos de execução

Decisão de simplicidade operacional: **um único binário**, **um `main.go`**, **um `Dockerfile`**. O modo de execução é escolhido por **argumento CLI** — sem segundo serviço, sem segundo image tag obrigatório.

### 9.1 Layout do repositório (v1)

```text
cmd/main.go      # único entrypoint
Dockerfile       # única imagem
go.mod
docs/specs/...
```

Pacotes internos (`internal/...`) podem existir para organização; o que importa para o usuário é: **uma imagem, dois modos**.

### 9.2 Argumentos

```text
audit <command>
```

| Command | Comportamento | Processo |
|---------|---------------|----------|
| `api` | Sobe o servidor HTTP (**somente ingestão**) | Long-running |
| `migrate` | Executa o job de arquivamento quente → S3 (uma passada) e **encerra** | Batch / CronJob |

- Sem command ou command desconhecido → exit ≠ 0 + help em stderr.
- Flags/env compartilhados (DB, S3, credenciais) valem para os dois modos; só o que cada modo precisa é validado no boot.

**Exemplos**

```bash
# API
docker run --rm -p 8080:8080 audit:latest api

# Job de cold storage (idempotente; seguro reexecutar)
docker run --rm audit:latest migrate
```

### 9.3 Dockerfile

- Multi-stage build Go → imagem final mínima (distroless/scratch/alpine).
- `ENTRYPOINT` = binário; **command** vem dos args (`api` ou `migrate`).
- Mesma imagem no **Container App** (`api`) e no **Container Apps Job** (`migrate`).

```dockerfile
# ilustrativo
ENTRYPOINT ["/audit"]
# uso: args ["api"] no Container App; args ["migrate"] no Container Apps Job
```

### 9.4 Orquestração sugerida (Azure)

| Workload | Args | Onde |
|----------|------|------|
| API long-running | `api` | **Azure Container Apps** (app) |
| Job agendado diário (ex. `0 3 * * *`) | `migrate` | **Azure Container Apps Jobs** (`--trigger-type Schedule`) |

CI/CD: `pipelines/pipe.yml` — testes → `go test -race` → push **ACR** → deploy API + Job (mesma imagem, args diferentes).

Isso reduz complexidade: um artefato para build/push/promote; só muda o arg (`api` vs `migrate`).

### 9.5 Por quê

- Menos imagens, menos pipelines, menos drift de versão entre “API” e “job”.
- Operação óbvia: `api` ou `migrate`.
- O job continua **idempotente** (seção 5.5); falha no CronJob → próxima execução retoma batches pendentes.

---

## 10. Fluxo de ingestão

```text
Client → Basic Auth → Validate body
       → Redact/mask secrets (tokens, keys — ver 12.7)
       → Extract hot columns
       → Split metadata (allowlist global) vs extensions
       → Sync INSERT Postgres (ver 10.1)
       → 202 + id
```

Idempotência: tabela/`UNIQUE (project_id, idempotency_key)` com TTL ou partição alinhada à retenção.

### 10.1 Orçamento de latência (≤ 100 ms p99)

O SLO cobre o caminho completo até a resposta HTTP de sucesso. Orçamento sugerido:

| Etapa | Orçamento |
|-------|-----------|
| Auth + parse/validate + redact secrets | ≤ 10 ms |
| Extrair colunas + montar JSONB | ≤ 5 ms |
| Sync INSERT Postgres | ≤ 70 ms |
| Margem / GC / jitter | ≤ 15 ms |
| **Total** | **≤ 100 ms** |

**Persistência (decisão 12.1): sync INSERT no Postgres**

| Modo | Semântica do `202` | Como atinge ≤ 100 ms |
|------|--------------------|----------------------|
| **Sync INSERT** | Evento **já durável no PG** | INSERT single-row + pool; DB na mesma região; índices mínimos no hot path |

Não há fila no hot path: durabilidade = commit no Postgres antes do `202`.

Métricas e alertas de latência: ver seção 11.

---

## 11. Observabilidade e objetivos (SLI / SLO)

Objetivo: em qualquer momento ser possível responder **sim/não** se o serviço está cumprindo as metas, com evidência em logs e métricas.

### Stack (decisão)

| Sinal | Tecnologia | Notas |
|-------|------------|--------|
| **Logs** | `log/slog` (Go stdlib) | Handler JSON (`slog.NewJSONHandler`) em stdout; attrs tipados |
| **Métricas** | OpenTelemetry Metrics (`go.opentelemetry.io/otel`) | Instruments no código; export **OTLP → Elastic** (APM/Metrics ou via Elastic OTel Collector) |

Mesmo setup nos modos `api` e `migrate` (logger + meter provider no boot de `main.go`). Traces ficam **fora da v1** (só logs + metrics), salvo se o Elastic APM for habilitado depois sem mudar o contrato da API.

### 11.1 API HTTP

#### Identificadores em toda request

| Campo | Origem | Regra |
|-------|--------|--------|
| `request_id` | Header `X-Request-ID` ou gerado no edge/handler | Sempre presente na resposta (`X-Request-ID`) e nos logs |
| `correlation_id` | Header `X-Correlation-ID` ou body (`correlation_id` no ingest) | Propagado nos logs; se ausente no ingest, pode ser igual ao `request_id` |
| `project_id` | Basic Auth username | Sempre no contexto |

Propagar `request_id` / `correlation_id` / `project_id` no `context.Context` e anexar ao `slog.Logger` da request (`logger.With(...)`), para não repetir attrs manualmente.

#### Logs estruturados — `slog` (JSON)

Todo log de request (início/fim/erro) inclui no mínimo:

```json
{
  "time": "2026-09-07T12:00:00.123Z",
  "level": "INFO",
  "msg": "request_completed",
  "request_id": "req_...",
  "correlation_id": "corr_...",
  "project_id": "billing",
  "method": "POST",
  "path": "/v1/events",
  "status": 202,
  "duration_ms": 42,
  "outcome": "success"
}
```

Em falha (`outcome=failure`): attrs `error_code`, `error` (mensagem segura, sem secret/PII desnecessário).  
Nunca logar password do Basic Auth nem body completo com dados sensíveis por padrão.

#### Métricas da API — OpenTelemetry

Instruments (nomes estáveis; attributes = labels):

| Instrumento OTel | Tipo | Attributes | Uso |
|------------------|------|------------|-----|
| `http.server.request.total` | Counter | `http.method`, `http.route`, `http.status_code`, `project_id`, `outcome` | Volume, sucesso/falha |
| `http.server.request.duration` | Histogram (s) | `http.method`, `http.route`, `project_id` | Latência p50/p95/**p99** |

- `outcome=success`: status 2xx; `outcome=failure`: 4xx/5xx (e erros antes do status).
- **RPM:** derivada no backend (`rate` do counter × 60) ou recording rule — não precisa de instrument separado.
- Export: OTLP (gRPC/HTTP) para o stack **Elastic** (endpoint/creds via env, ex. `OTEL_EXPORTER_OTLP_ENDPOINT`).

#### SLIs / SLOs (quando o objetivo está OK)

| Objetivo | SLI | SLO v1 | Como ver |
|----------|-----|--------|----------|
| Escrita rápida | p99 de `POST /v1/events` (server-side) | **≤ 100 ms** | histograma OTel / dashboard |
| Disponibilidade da API | % `outcome=success` | **≥ 99.9%** / 30d (excl. 401 por credencial inválida do cliente, se desejado) | `success / (success+failure)` |
| Volume | RPM por endpoint e por `project_id` | baseline + alerta de queda/spike anômalo | série RPM |

**Dashboards mínimos (Elastic):** RPM, latência (p50/p99), taxa sucesso vs falha, para `POST /v1/events`.  
**Alertas mínimos:** p99 ingestão > 100 ms por N minutos; taxa de falha 5xx > limiar; RPM = 0 inesperado.

### 11.2 Job de arquivamento (quente → frio)

#### Logs estruturados — `slog`

Cada run e cada batch emitem JSON via `slog` com:

```json
{
  "time": "...",
  "level": "ERROR",
  "msg": "cold_archive_batch_failed",
  "job_id": "...",
  "run_id": "...",
  "partition_name": "events_2026_06",
  "batch_seq": 3,
  "s3_key": "...",
  "error_code": "s3_upload",
  "error": "PutObject timeout after 30s",
  "rows": 100000,
  "bytes": 52428800,
  "duration_ms": 12004
}
```

Sucesso de run: `msg=cold_archive_run_completed` com totais do dia (`partitions_dropped`, `rows_total`, `bytes_total`, `duration_ms`).

#### Métricas do job — OpenTelemetry

| Instrumento OTel | Tipo | Attributes | Uso |
|------------------|------|------------|-----|
| `cold_archive.run.duration` | Histogram (s) | `status` | **Tempo de migração** por run |
| `cold_archive.batch.duration` | Histogram (s) | `partition_name`, `status` | Tempo por lote |
| `cold_archive.rows.migrated` | Counter | `partition_name` | **Quantidade** de linhas migradas |
| `cold_archive.bytes.migrated` | Counter | `partition_name` | **Volume** (bytes) migrado |
| `cold_archive.partitions.dropped` | Counter | — | Partições efetivamente removidas do PG |
| `cold_archive.failures` | Counter | `error_code`, `stage` (`read`\|`upload`\|`verify`\|`drop`) | Falhas com motivo agregável |
| `cold_archive.batches.pending` | UpDownCounter / Observable gauge | `partition_name` | Trabalho restante (idempotência visível) |

**Derivadas diárias (dashboard / recording rules no backend):**

- `rows_migrated_daily` = aumento de `cold_archive.rows.migrated` em 24h  
- `bytes_migrated_daily` = aumento de `cold_archive.bytes.migrated` em 24h  
- `migration_duration_daily` = duração do run (ou soma dos runs do dia)

Flush/shutdown do MeterProvider no fim do `migrate` (processo batch) para não perder o último export OTLP.

#### SLIs / SLOs do cold path

| Objetivo | SLI | SLO v1 | Como ver |
|----------|-----|--------|----------|
| Arquivamento em dia | partições elegíveis sem `dropped` há > 48h | **0** | gauge/alerta + `cold_archive_jobs` |
| Confiabilidade do job | runs com `status=failed` | investigar todo failed; retry sem reprocessar verified | logs `error_code` + `cold_archive.failures` |
| Transparência de volume | rows/bytes migrados por dia | sempre visível no dashboard | métricas diárias |

### 11.3 Critério “estamos no objetivo?”

Checklist operacional (verde = OK):

1. p99 `POST /v1/events` ≤ 100 ms  
2. Taxa de sucesso da API dentro do SLO  
3. RPM coerente com o tráfego esperado (sem blackhole)  
4. Job diário rodou; falhas (se houver) têm `error_code` e só batches pendentes restam  
5. Volume diário migrado para S3 aparece no dashboard (rows + bytes)

---

## 12. Decisões

Todas as decisões da rodada estão **fechadas**:

| # | Tema | Decisão |
|---|------|---------|
| 12.1 | Persistência hot path | **Sync INSERT no Postgres** — `202` só após commit; evento durável no PG |
| 12.2 | Allowlist de metadata | **Global** |
| 12.3 | Leitura / SQL admin | **Fora deste serviço** (outro sistema) |
| 12.4 | Idempotency-Key | **Opcional**; senão `sha256(project_id + canonical_json(body))` |
| 12.5 | Partição | **Semanal** (`PARTITION BY RANGE (occurred_at)`) |
| 12.6 | Compressão Parquet | **zstd** — prioriza densidade e custo de storage (CPU/throughput do `migrate` em segundo plano) |
| 12.7 | PII / segredos | **SSE-KMS/CMK** no frio + **máscara** de tokens/chaves na ingestão; S3 e Azure |
| 12.8 | Leitura de frio | **Fora de escopo** |
| 12.9 | Métricas | **OpenTelemetry → Elastic** |

### 12.7 Detalhe — criptografia e máscara

**Em repouso (object storage)**

- AWS: upload via AWS SDK com **SSE-KMS** (CMK).
- Azure: upload via Azure SDK com encryption / **CMK no Key Vault**.
- Interface única no código; provider selecionado por config.

**Na ingestão (antes do INSERT)**

Camada de validação/redação no body (`metadata`, `extensions` e campos texto conhecidos):

- Detectar padrões de **token**, **API key**, **Bearer**, secrets óbvios (`aws_secret_access_key`, `private_key`, JWT, etc.).
- Valor mascarado de forma irreversível no store (ex.: `***REDACTED***` ou hash truncado), **nunca** gravar o segredo em claro no PG nem no Parquet.
- Logar apenas que houve redação (`fields_redacted_count`), sem o valor original.
- Deve caber no orçamento de latência (parte dos ≤ 10 ms de parse/validate).

**Fora do escopo 12.7:** criptografia campo-a-campo no PG; leitura/desmascaramento (não há API de leitura).

---

## 13. Critérios de aceite (v1)

- [ ] `POST /v1/events` autentica via Basic Auth e grava `project_id` a partir do username.
- [ ] Body inválido ou sem `action`/`occurred_at` retorna `400`.
- [ ] Evento persistido no PG com colunas tipadas + `metadata`/`extensions` JSONB.
- [ ] Allowlist de metadata **global** aplicada; demais chaves em `extensions`.
- [ ] Idempotência: header opcional; se ausente, chave derivada de `project_id` + body canônico; sem duplicar eventos.
- [ ] Latência server-side de `POST /v1/events` ≤ 100 ms no p99 sob carga alvo (métrica + teste de carga).
- [ ] **Sem** endpoints de leitura de eventos neste serviço.
- [ ] Particionamento **semanal** por `occurred_at` ativo.
- [ ] Parquet no frio com compressão **zstd**.
- [ ] Upload frio com **SSE-KMS** (S3) ou **CMK** (Azure Blob), via SDKs oficiais; provider configurável.
- [ ] Máscara na ingestão de tokens/chaves sensíveis antes do INSERT.
- [ ] Job diário `migrate` **idempotente**; exporta Parquet (zstd) para S3 ou Azure; valida row_count/checksum; só então `DROP PARTITION`.
- [ ] `POST /v1/events` usa **sync INSERT**; `202` implica evento no PG.
- [ ] Falhas do job registram `error_code` + motivo em log estruturado e em `cold_archive_*`.
- [ ] Métricas do job: duração da migração, rows e bytes migrados (visíveis em agregação diária).
- [ ] Logs via **`log/slog`** (JSON) com `request_id` e `correlation_id`.
- [ ] Métricas via **OpenTelemetry → Elastic** (OTLP): RPM derivável, latência p99, sucesso/falha.
- [ ] Dashboard/alertas no Elastic cobrem o checklist da seção 11.3.
- [ ] Binário único com args `api` e `migrate`; um `Dockerfile`; mesma imagem para Deployment e CronJob.
- [ ] Documentação OpenAPI alinhada a esta spec (somente ingestão).


---

## 14. Próximos passos

1. Implementar abstração `BlobStore` (S3 SSE-KMS + Azure CMK) e redactor de secrets.
2. Spec de schema SQL + migrations (partições + `cold_archive_jobs` + `cold_archive_batches`).
3. Spec de credenciais/projetos (`project_id` + secret) e config de cold storage (S3/Azure + KMS).
4. Implementar `cmd/main.go` com dispatch `api` | `migrate` + `Dockerfile` único.
5. Implementar modo `migrate` (object storage, validação, DROP, idempotência por batch) reutilizando o mesmo código de store.
6. Spec/runbook de observabilidade no **Elastic** (dashboards, alertas, RPM e volume diário).
7. Manifests / pipeline: `pipelines/pipe.yml` (ACR + Container App API + Container Apps Job `migrate`).
