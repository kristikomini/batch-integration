# High-throughput nightly reconciliation engine — Spring Batch + JVM tuning

Every night, banks, credit bureaus (CRIF), utilities and retailers process **millions of rows**
of transactions, inventory syncs or billing records from flat files, validate them, reconcile
against a database, and quarantine anomalies. The junior approach — read the file into a `List`
and call `repository.saveAll()` — dies with `OutOfMemoryError` and takes hours.

This is a **fault-tolerant Spring Batch** engine that ingests a **5-million-row** CSV/fixed-width
file with **flat heap** (runs under `-Xmx256m`), validates business rules, reconciles against
PostgreSQL, and routes anomalies to a **dead-letter table** — restartably, and fast.

Target market: FinTech, credit bureaus, banking, utilities, retail (Milano, Bologna, Verona).

> Follows the shared [engineering standards](../ENGINEERING-STANDARDS.md). The headline claim is
> a measured [benchmark](docs/BENCHMARK.md), not a feature list.

## Enterprise features

- **Chunk-oriented processing** — custom `ItemReader` streaming the file with **Java NIO**
  (never loading it whole), `ItemProcessor` for validation/mapping, and a **JDBC-batch**
  `ItemWriter` (batched `INSERT`s — **~13× faster** than row-by-row JPA in the [benchmark](docs/BENCHMARK.md)).
- **Partitioning & multithreading** — the file is split across worker threads via a
  `TaskExecutor` (~3× again, measured); thread-safe by construction (contiguous, non-overlapping ranges).
- **Restartability** — if the job dies at row 3,400,000 on a DB timeout, restarting resumes from
  the **last committed chunk** (Spring Batch `JobRepository`), never from zero and never
  double-inserting. → [`docs/BATCH-DESIGN.md`](docs/BATCH-DESIGN.md)
- **Skip / retry / fail by cause** — malformed row → **skip** (bounded, to dead-letter);
  transient DB error → **retry** (bounded, backoff); missing mandatory column → **fail** the job.
  Conflating these is the usual bug.
- **Idempotent re-ingest** — a re-sent file is a no-op (dedupe on natural key + file `sha256`).
- **Reconciliation report** — rows read / imported / skipped-by-reason, plus a quarantine file
  of rejected rows for the supplier.

## The headline benchmark (measured)

Under `-Xmx256m`, PostgreSQL 16 in Docker (full details + method in [`docs/BENCHMARK.md`](docs/BENCHMARK.md)):

| Approach | Throughput | Heap | Result |
|----------|-----------|------|--------|
| Naive: `readAll()` → JPA `saveAll()` (5M) | — | 256 MB | **OutOfMemoryError** |
| Streaming + per-row JPA | 1,309 rows/s | flat 256 MB | full |
| Streaming + JDBC batch (1 thread) | 17,296 rows/s | flat 256 MB | full (~13× per-row JPA) |
| + partitioning (4 threads) | 52,279 rows/s | flat 256 MB | full (~3× again) |
| **Partitioned, 5M rows** | 45,027 rows/s | flat 256 MB | **full in 111 s** |

## What this demonstrates (CV bullets)

*Proven by tests in this repo (Postgres via Testcontainers in CI; end-to-end run verified against
a real Postgres locally):*
- Built a **partitioned, fault-tolerant Spring Batch** engine that streams a delimited feed with a
  custom NIO `ItemReader` (one line in memory at a time — flat heap at any file size) and writes
  through a **JDBC-batch** upsert (`ON CONFLICT DO NOTHING`), so ingest is both fast and idempotent
  on the natural key.
- Split fault handling **by cause**: a malformed row is skipped (bounded) to a **dead-letter table**
  and a transient DB error is retried (bounded), while a broken-contract file fails the job — proven
  by a 1,000-row run that imported 956 and quarantined 44 with a per-reason breakdown
  (`COLUMN_COUNT`, `BAD_DATE`, `NON_POSITIVE_AMOUNT`, `BAD_CURRENCY`), summing back to 1,000.
- Made the job **restartable** (chunk-committed reader position in the `JobRepository`) and the
  whole ingest **file-level idempotent** (`sha256` ledger): a byte-identical re-drop is a no-op,
  verified by re-ingesting the same file and asserting zero new rows.
- Partitioned the work across a bounded worker-thread pool (default 4), with a reconciliation
  report (rows read / imported / skipped-by-reason) stamped onto the `import_file` ledger.

*Measured (see [`docs/BENCHMARK.md`](docs/BENCHMARK.md)):* 5M rows in ~111 s at a flat 256 MB heap;
naive `saveAll()` OOMs; JDBC-batch ~13× per-row JPA; partitioning ~3× again on a 4-core box.

## Run it

```bash
docker compose up --build   # postgres + the app, ingesting a bundled sample feed under -Xmx256m
```

The app ingests `sample-data/feed.csv` on startup and writes the reconciliation report to
the log and the `import_file` table. To generate a large file for the benchmark:

```bash
java -cp target/classes it.kristikomini.batch.tools.SampleDataGenerator big.csv 5000000 0.001
```

In production the feed arrives over SFTP into a landing directory (`spring-integration-sftp`
is on the classpath); the demo mounts a file and drives the same `FileIngestService` directly.

## Course topics exercised

File NIO & buffered streams, custom `Spliterator`/Stream API, `ExecutorService` & thread pools,
GC behaviour & heap profiling, checked-vs-unchecked exception design (retryable vs fatal).
Academy modules: 25 (SQL & migrations), 23 (transactions), 12 (expected failure), 08
(concurrency), 09 (memory & GC).
