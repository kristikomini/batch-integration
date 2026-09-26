package it.kristikomini.batch;

import it.kristikomini.batch.ingest.FileIngestService;
import it.kristikomini.batch.tools.SampleDataGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end: run the real partitioned job over a generated feed against a real
 * Postgres, and assert the reconciliation invariants — every row is either imported
 * or dead-lettered, the counts add up, and a re-drop of the same bytes is a no-op.
 *
 * <p>Needs Docker, so it is skipped locally (the Maven-forked JVM cannot reach the
 * Windows Docker pipe) and runs in CI, where Docker is available.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class ReconciliationJobIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("reconciliation")
            .withUsername("app")
            .withPassword("app");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    FileIngestService ingestService;

    @Autowired
    JdbcTemplate jdbc;

    @TempDir
    Path tmp;

    private static final int ROWS = 1000;

    /**
     * The container is shared across tests; reset both the business tables and the
     * Spring Batch metadata before each test, so every test starts from empty and
     * a re-used file hash targets a fresh job instance.
     */
    @BeforeEach
    void clean() {
        jdbc.execute("""
                TRUNCATE transaction_record, rejected_row, import_file,
                         batch_step_execution_context, batch_step_execution,
                         batch_job_execution_context, batch_job_execution_params,
                         batch_job_execution, batch_job_instance
                RESTART IDENTITY CASCADE
                """);
    }

    private Path sampleFile() throws IOException {
        Path f = tmp.resolve("feed.csv");
        SampleDataGenerator.write(f, ROWS, 0.05, 7L); // ~5% deliberately malformed, deterministic
        return f;
    }

    @Test
    void importsGoodRowsDeadLettersBadOnesAndBalances() throws Exception {
        Path file = sampleFile();

        FileIngestService.Result result = ingestService.ingest(file);

        assertThat(result.outcome()).isEqualTo(FileIngestService.Outcome.RAN);
        assertThat(result.execution().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        long imported = count("SELECT count(*) FROM transaction_record");
        long rejected = count("SELECT count(*) FROM rejected_row");

        // The core reconciliation invariant: nothing is silently lost.
        assertThat(imported + rejected).isEqualTo(ROWS);
        assertThat(rejected).isPositive(); // the salted-in bad rows were caught, not imported

        // The ledger reflects the same numbers.
        assertThat(count("SELECT rows_read FROM import_file WHERE sha256 = ?", result.sha256())).isEqualTo(ROWS);
        assertThat(count("SELECT rows_imported FROM import_file WHERE sha256 = ?", result.sha256())).isEqualTo(imported);
        assertThat(count("SELECT rows_skipped FROM import_file WHERE sha256 = ?", result.sha256())).isEqualTo(rejected);
        assertThat(jdbc.queryForObject("SELECT status FROM import_file WHERE sha256 = ?", String.class, result.sha256()))
                .isEqualTo("COMPLETED");
    }

    @Test
    void reIngestingTheSameFileIsANoOp() throws Exception {
        Path file = sampleFile();

        ingestService.ingest(file);
        long afterFirst = count("SELECT count(*) FROM transaction_record");

        FileIngestService.Result second = ingestService.ingest(file);

        assertThat(second.outcome()).isEqualTo(FileIngestService.Outcome.SKIPPED_ALREADY_INGESTED);
        assertThat(count("SELECT count(*) FROM transaction_record")).isEqualTo(afterFirst); // no duplicates
    }

    private long count(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0 : n;
    }
}
