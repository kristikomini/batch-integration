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

## Results (measured)

Measured on Windows 11, JDK 21, PostgreSQL 16 in Docker, `-Xmx256m`, single indicative run each
(not the 3×-median protocol above — good enough to show the shape; re-run with the protocol for a
report). Dataset sizes differ by necessity: naive `saveAll` **cannot** run at 5M (it OOMs), and
per-row JPA is too slow to take to 5M — so the comparable metric across rows is **rows/s**.

| # | Approach | Dataset | Wall-clock | rows/s | Heap | Result |
|---|----------|---------|-----------|--------|------|--------|
| 1 | Naive: `readAll()` → JPA `saveAll()` | 5M | — | — | 256 MB | **OutOfMemoryError** (0 rows) |
| 2 | Streaming + per-row JPA `persist` (flush/clear) | 100k | 76.4 s | **1,309** | flat 256 MB | full |
| 3 | Streaming + JDBC batch, 1 thread | 500k | 28.9 s | **17,296** | flat 256 MB | full |
| 4 | + partitioning, 4 threads | 500k | 9.6 s | **52,279** | flat 256 MB | full |
| — | approach 4 **at 5M rows** | 5M | 111 s | **45,027** | flat 256 MB | full (4,994,940 imported) |

Takeaways from these numbers:
- **Naive OOMs.** Reading 5M rows into a `List` blows the 256 MB heap before a single insert — the
  whole reason for a streaming reader.
- **The JDBC batch writer is the big win:** ~**13×** over per-row JPA (17,296 vs 1,309 rows/s), same
  flat heap. (The gap widens with a fatter row / more indexes; per-row JPA also can't reach 5M in
  reasonable time.)
- **Partitioning adds ~3×** (52,279 vs 17,296) on this 4-core box, until the DB write path becomes
  the ceiling — the signal to stop adding threads.
- **Heap stays flat** at every streaming approach: the 5M run completes under `-Xmx256m`.

## The one-line story for the interview
> "Naive `saveAll()` OOMs at 5M rows under 256 MB. Streaming keeps the heap flat; the real win is
> the JDBC batch writer — about 13× over per-row JPA in my measurement — and partitioning got me
> another ~3× on a 4-core box, up to where the DB write path saturates. The full 5M file loads in
> ~111 s at a steady 256 MB heap."
