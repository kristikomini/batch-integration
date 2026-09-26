package it.kristikomini.batch.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * The single place that decides whether — and how — to ingest a file. It owns
 * <b>file-level idempotency</b>: a byte-identical re-drop is a no-op, a changed file
 * is reprocessed, and a previously-failed file resumes rather than starting over.
 *
 * <p>Why here and not in the job: the launch decision (skip / resume / run) depends
 * on the file's {@code sha256} and the {@code import_file} ledger, which is business
 * bookkeeping, not batch mechanics. Keeping it out of the job also lets tests drive
 * the job directly with deterministic parameters.
 */
@Service
public class FileIngestService {

    private static final Logger log = LoggerFactory.getLogger(FileIngestService.class);

    public enum Outcome { RAN, SKIPPED_ALREADY_INGESTED }

    public record Result(Outcome outcome, JobExecution execution, String sha256) {}

    private final JobLauncher jobLauncher;
    private final Job reconciliationJob;
    private final JdbcTemplate jdbc;

    public FileIngestService(JobLauncher jobLauncher, Job reconciliationJob, JdbcTemplate jdbc) {
        this.jobLauncher = jobLauncher;
        this.reconciliationJob = reconciliationJob;
        this.jdbc = jdbc;
    }

    public Result ingest(Path file) throws Exception {
        String sha256 = sha256(file);
        String fileName = file.getFileName().toString();

        // Idempotency gate: an identical file that already COMPLETED is a no-op.
        List<String> existing = jdbc.queryForList(
                "SELECT status FROM import_file WHERE sha256 = ?", String.class, sha256);
        if (existing.contains("COMPLETED")) {
            log.info("File {} (sha256={}) already ingested — skipping.", fileName, shortSha(sha256));
            return new Result(Outcome.SKIPPED_ALREADY_INGESTED, null, sha256);
        }
        if (existing.isEmpty()) {
            // First time we see these bytes: open the ledger row. A later re-drop of the same
            // bytes after a FAILED run will find this row and the job will resume its instance.
            jdbc.update("""
                    INSERT INTO import_file (file_name, sha256, status, started_at)
                    VALUES (?, ?, 'STARTED', now())
                    """, fileName, sha256);
        }

        // Both parameters are identifying, so re-launching the same bytes targets the same
        // JobInstance — that is what turns a re-run of a FAILED file into a resume.
        JobParameters params = new JobParametersBuilder()
                .addString("filePath", file.toAbsolutePath().toString())
                .addString("sha256", sha256)
                .toJobParameters();

        JobExecution execution = jobLauncher.run(reconciliationJob, params);
        return new Result(Outcome.RAN, execution, sha256);
    }

    private static String sha256(Path file) {
        try (InputStream in = Files.newInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (DigestInputStream dis = new DigestInputStream(in, digest)) {
                byte[] buffer = new byte[1 << 16];
                while (dis.read(buffer) != -1) {
                    // streaming: never holds the whole file in memory
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot hash file: " + file, e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static String shortSha(String sha) {
        return sha.substring(0, 12);
    }
}
