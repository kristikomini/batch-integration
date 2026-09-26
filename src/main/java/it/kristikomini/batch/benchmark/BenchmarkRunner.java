package it.kristikomini.batch.benchmark;

import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.BufferedReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Benchmark harness for {@code docs/BENCHMARK.md}. Activated only with {@code --bench.mode=...};
 * the normal app and the tests never trigger it. It loads the same file two "wrong" ways so the
 * numbers next to the real Spring Batch job (streaming + JDBC batch + partitioning) are measured
 * on the same machine and file, not asserted.
 *
 * <pre>
 *   java -Xmx256m -jar app.jar --bench.mode=naive     --bench.file=big.csv
 *   java -Xmx256m -jar app.jar --bench.mode=jpa-stream --bench.file=big.csv
 * </pre>
 */
@Component
public class BenchmarkRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);

    private final TxnJpaRepository jpaRepository;
    private final JdbcTemplate jdbc;
    private final EntityManager entityManager;
    private final TransactionTemplate txTemplate;

    public BenchmarkRunner(TxnJpaRepository jpaRepository, JdbcTemplate jdbc,
                           EntityManager entityManager, PlatformTransactionManager txManager) {
        this.jpaRepository = jpaRepository;
        this.jdbc = jdbc;
        this.entityManager = entityManager;
        this.txTemplate = new TransactionTemplate(txManager);
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!args.containsOption("bench.mode")) {
            return; // not a benchmark run
        }
        String mode = args.getOptionValues("bench.mode").get(0);
        Path file = Path.of(args.getOptionValues("bench.file").get(0));

        jdbc.execute("TRUNCATE transaction_record");
        long maxHeapMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        log.info("BENCHMARK mode={} file={} maxHeap={}MB", mode, file, maxHeapMb);

        long start = System.nanoTime();
        long rows;
        try {
            rows = switch (mode) {
                case "naive" -> naiveSaveAll(file);
                case "jpa-stream" -> jpaStream(file);
                default -> throw new IllegalArgumentException("unknown mode: " + mode);
            };
        } catch (OutOfMemoryError oom) {
            long imported = jdbc.queryForObject("SELECT count(*) FROM transaction_record", Long.class);
            log.error("BENCHMARK result mode={} -> OutOfMemoryError (imported {} before OOM, heap {}MB)",
                    mode, imported, maxHeapMb);
            System.exit(1);
            return;
        }

        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        log.info("BENCHMARK result mode={} rows={} seconds={} rowsPerSec={} heap={}MB",
                mode, rows, String.format("%.1f", seconds),
                String.format("%.0f", rows / seconds), maxHeapMb);
        System.exit(0);
    }

    /** Approach 1: read the whole file into a List and hand it to {@code saveAll} — the OOM path. */
    private long naiveSaveAll(Path file) {
        List<TxnEntity> all = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            r.readLine(); // header
            String line;
            while ((line = r.readLine()) != null) {
                TxnEntity e = parse(line);
                if (e != null) {
                    all.add(e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        jpaRepository.saveAll(all);
        return all.size();
    }

    /** Approach 2: stream the file, JPA {@code persist} per row, flush/clear to keep heap flat. */
    private long jpaStream(Path file) {
        return txTemplate.execute(status -> {
            long count = 0;
            try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                r.readLine(); // header
                String line;
                while ((line = r.readLine()) != null) {
                    TxnEntity e = parse(line);
                    if (e == null) {
                        continue;
                    }
                    entityManager.persist(e);
                    if (++count % 500 == 0) {
                        entityManager.flush();
                        entityManager.clear();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return count;
        });
    }

    /** Lenient parse: skip malformed rows silently (the benchmark measures throughput, not validation). */
    private static TxnEntity parse(String line) {
        String[] c = line.split(",", -1);
        if (c.length != 6) {
            return null;
        }
        try {
            return new TxnEntity(c[0], c[1], new BigDecimal(c[2]), c[3].toUpperCase(),
                    LocalDate.parse(c[4]), c[5], "benchmark");
        } catch (RuntimeException e) {
            return null;
        }
    }
}
