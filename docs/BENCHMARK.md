# Benchmark — why the design choices pay off

The point of this repo is a number a tech lead believes. Everything here is measured on the same
machine, same 5M-row file, `-Xmx256m`, PostgreSQL in Docker.

## Method
- Generate a deterministic 5M-row file (`scripts/gen-sample.sh`, seeded).
- Run each approach 3×, discard the first (warm-up), report the median.
- Capture: wall-clock, peak heap (`-Xlog:gc` / JFR), GC pause total, rows/s.

## Approaches compared

1. **Naive** — read whole file into `List<Row>`, `repository.saveAll(list)`.
   *Expected:* OOM before it finishes, or extreme GC thrash if the heap is raised.
2. **Streaming + JPA** — NIO streaming reader, JPA `save()` per row.
   *Expected:* flat heap, but slow (one round-trip + Hibernate overhead per row).
3. **Streaming + JDBC batch** — NIO reader, chunked JDBC batch `INSERT` (`rewriteBatchedStatements`).
   *Expected:* flat heap, ~100× the throughput of (2).
4. **(3) + partitioning** — split by file offset across N worker threads.
   *Expected:* near-linear speedup until the DB write path saturates.

## Results (fill in)

| # | Approach | Wall-clock | Peak heap | GC total | rows/s |
|---|----------|-----------|-----------|----------|--------|
| 1 | Naive saveAll | `<OOM>` | — | — | — |
| 2 | Stream + JPA | `<N>` | `<N>` MB | `<N>` ms | `<N>` |
| 3 | Stream + JDBC batch | `<N>` | `<N>` MB | `<N>` ms | `<N>` |
| 4 | + partitioning (`<N>` threads) | `<N>` | `<N>` MB | `<N>` ms | `<N>` |

## The one-line story for the interview
> "Naive `saveAll()` OOMs at 5M rows under 256 MB. Streaming keeps the heap flat; the real win is
> the JDBC batch writer — about 100× over per-row JPA — and partitioning gets me another `<N>`× up
> to where the DB write path saturates, which is the point to stop adding threads."
