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
  `ItemWriter` (batched `INSERT`s — the ~100× speedup over row-by-row JPA `saveAll()`).
- **Partitioning & multithreading** — the file is split across worker threads via a
  `TaskExecutor`, using thread-safe collections (`ConcurrentHashMap`, `BlockingQueue`).
- **Restartability** — if the job dies at row 3,400,000 on a DB timeout, restarting resumes from
  the **last committed chunk** (Spring Batch `JobRepository`), never from zero and never
  double-inserting. → [`docs/BATCH-DESIGN.md`](docs/BATCH-DESIGN.md)
- **Skip / retry / fail by cause** — malformed row → **skip** (bounded, to dead-letter);
  transient DB error → **retry** (bounded, backoff); missing mandatory column → **fail** the job.
  Conflating these is the usual bug.
- **Idempotent re-ingest** — a re-sent file is a no-op (dedupe on natural key + file `sha256`).
- **Reconciliation report** — rows read / imported / skipped-by-reason, plus a quarantine file
  of rejected rows for the supplier.

## The headline benchmark (fill after building)

Running the same 5M-row file two ways, `-Xmx256m`:

| Approach | Time | Peak heap | GC pauses | Result |
|----------|------|-----------|-----------|--------|
| Naive: `readAll()` → JPA `saveAll()` | `<hh:mm>` / OOM | `<N>` MB | `<N>` | crashes / hours |
| Spring Batch + NIO stream + JDBC batch + partitioning | `<mm:ss>` | `~stable` | `<N>` | full |

## What this demonstrates (CV bullets)

- Built a restartable Spring Batch engine ingesting 5M-row files under `-Xmx256m` (NIO streaming +
  JDBC batch writer), `<N>`× faster than naive JPA `saveAll()` which OOMs; a killed run resumes
  from the last committed chunk with zero duplicates.
- Partitioned the job across `<N>` worker threads; reconciled against PostgreSQL and routed
  `<N>` anomaly types to a dead-letter table with a per-reason reconciliation report.

## Run it

```bash
docker compose up   # app + postgres + SFTP container seeded with a sample file
```

## Course topics exercised

File NIO & buffered streams, custom `Spliterator`/Stream API, `ExecutorService` & thread pools,
GC behaviour & heap profiling, checked-vs-unchecked exception design (retryable vs fatal).
Academy modules: 25 (SQL & migrations), 23 (transactions), 12 (expected failure), 08
(concurrency), 09 (memory & GC).
