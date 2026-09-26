-- Reconciliation schema. Spring Batch's own JobRepository tables are created separately
-- (spring.batch.jdbc.initialize-schema); this migration owns the business tables.

-- The imported transactions. txn_id is the natural key: the writer upserts on it
-- (ON CONFLICT (txn_id) DO NOTHING), which is what makes a re-ingest idempotent.
CREATE TABLE transaction_record (
    txn_id       TEXT           PRIMARY KEY,
    account      TEXT           NOT NULL,
    amount       NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency     CHAR(3)        NOT NULL,
    value_date   DATE           NOT NULL,
    counterparty TEXT           NOT NULL,
    source_file  TEXT           NOT NULL,
    imported_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- The ledger of files we have seen: file-level idempotency and the per-run reconciliation counts.
-- sha256 is UNIQUE so an identical re-drop is detected and the report update targets one row.
CREATE TABLE import_file (
    id            BIGSERIAL   PRIMARY KEY,
    file_name     TEXT        NOT NULL,
    sha256        TEXT        NOT NULL UNIQUE,
    status        TEXT        NOT NULL,
    rows_read     BIGINT,
    rows_imported BIGINT,
    rows_skipped  BIGINT,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ
);

-- Dead-letter table: every skipped row lands here with its reason, so nothing is lost silently
-- and the supplier gets a per-reason quarantine.
CREATE TABLE rejected_row (
    id           BIGSERIAL   PRIMARY KEY,
    source_file  TEXT        NOT NULL,
    line_number  BIGINT,
    reason       TEXT        NOT NULL,
    raw_line     TEXT,
    error_detail TEXT,
    rejected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rejected_source_reason ON rejected_row (source_file, reason);
