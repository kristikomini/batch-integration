# Batch design

## The job

```
SFTP poll ─▶ file in landing/ ─▶ [Spring Batch job] ─▶ Postgres
                                     │
              chunk( read CSV → validate/map → upsert ), commit every N rows
                                     │
              ├─ ok        ─▶ imported
              ├─ bad row   ─▶ skipped (written to rejected/) — bounded skip limit
              └─ transient ─▶ retried (bounded), then skipped or fail
   on success: move file → archive/     on hard failure: move file → quarantine/
   always: write reconciliation report
```

## Why Spring Batch and not a `main()` loop

- **Restartability**: the `JobRepository` persists step execution state. A job that dies at
  chunk 400 resumes at chunk 400 on the next run — with commit-per-chunk semantics, no row is
  imported twice and none is skipped.
- **Skip / retry policy** is declarative and separated by *cause*:
  - malformed row → **skip** (bounded `skipLimit`) — it is a data problem, one row.
  - deadlock / lock timeout → **retry** (bounded) — transient.
  - missing mandatory column / unreadable file → **fail the job** — contract problem, stop.
- **Chunked + streaming** reader keeps memory flat regardless of file size.

## Idempotency

Files get re-sent. Dedupe on `(natural_key)` with an upsert, and record processed files by
`(filename, sha256)` so a byte-identical re-drop is a no-op and a changed file is reprocessed.

## Observability

Expose job/step metrics (rows read/written/skipped, duration) via Micrometer; a failed job is
an alert, not a silent nightly gap.

## Test

- Unit: the row mapper/validator against good and malformed rows.
- Integration (Testcontainers Postgres): run the job over a sample file; assert counts and the
  reconciliation report. Kill mid-job and restart; assert resume + no duplicates.
