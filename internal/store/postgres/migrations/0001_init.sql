-- Spec 001 seção 6.1 — camada quente.
--
-- A tabela é particionada por RANGE (occurred_at) em janelas semanais ISO;
-- a partição é a unidade de export e DROP do job `migrate` (seção 5.4).
-- Como o PostgreSQL exige que índices únicos de tabelas particionadas
-- contenham a chave de partição, `occurred_at` entra na PK e no índice de
-- idempotência.

CREATE TABLE IF NOT EXISTS events (
    id              uuid        NOT NULL,
    project_id      text        NOT NULL,
    idempotency_key text        NOT NULL,
    schema_version  smallint    NOT NULL,
    action          text        NOT NULL,
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
    metadata        jsonb       NOT NULL DEFAULT '{}',
    extensions      jsonb       NOT NULL DEFAULT '{}',
    PRIMARY KEY (id, occurred_at)
) PARTITION BY RANGE (occurred_at);

-- Idempotência com escopo por projeto (seção 4.1).
CREATE UNIQUE INDEX IF NOT EXISTS events_idempotency_uk
    ON events (project_id, idempotency_key, occurred_at);

-- Índices do hot path mantidos ao mínimo para caber no orçamento de INSERT.
CREATE INDEX IF NOT EXISTS events_project_occurred_at_idx
    ON events (project_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS events_project_action_occurred_at_idx
    ON events (project_id, action, occurred_at);

-- Credenciais Basic Auth quando AUTH_CREDENTIALS_SOURCE=db (seção 3).
CREATE TABLE IF NOT EXISTS services (
    project_id  text PRIMARY KEY,
    secret_hash text        NOT NULL,
    disabled    boolean     NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Controle do arquivamento quente → frio (seção 5.5).
CREATE TABLE IF NOT EXISTS cold_archive_jobs (
    id               uuid        PRIMARY KEY,
    partition_name   text        NOT NULL UNIQUE,
    occurred_at_from timestamptz NOT NULL,
    occurred_at_to   timestamptz NOT NULL,
    status           text        NOT NULL,
    s3_prefix        text,
    row_count        bigint,
    bytes_total      bigint,
    error            text,
    error_code       text,
    started_at       timestamptz,
    finished_at      timestamptz,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT cold_archive_jobs_status_chk
        CHECK (status IN ('pending', 'exporting', 'exported', 'dropped', 'failed'))
);

CREATE INDEX IF NOT EXISTS cold_archive_jobs_status_idx
    ON cold_archive_jobs (status, occurred_at_to);

CREATE TABLE IF NOT EXISTS cold_archive_batches (
    id               uuid        PRIMARY KEY,
    job_id           uuid        NOT NULL REFERENCES cold_archive_jobs (id) ON DELETE CASCADE,
    batch_seq        int         NOT NULL,
    status           text        NOT NULL,
    s3_key           text        NOT NULL,
    row_count        bigint,
    bytes            bigint,
    sha256           text,
    occurred_at_from timestamptz,
    occurred_at_to   timestamptz,
    -- id da última linha do lote: com occurred_at_to forma o cursor keyset
    -- exato, permitindo retomar após um lote `verified` sem reler o PG.
    cursor_id        uuid,
    error            text,
    error_code       text,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, batch_seq),
    UNIQUE (s3_key),
    CONSTRAINT cold_archive_batches_status_chk
        CHECK (status IN ('pending', 'uploaded', 'verified', 'failed'))
);

CREATE INDEX IF NOT EXISTS cold_archive_batches_job_status_idx
    ON cold_archive_batches (job_id, status);
