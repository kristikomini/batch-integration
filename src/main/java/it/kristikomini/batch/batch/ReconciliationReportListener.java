package it.kristikomini.batch.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * Turns a finished job into a reconciliation report: how many rows were read,
 * imported, and skipped — and, for the skipped ones, the breakdown by reason.
 *
 * <p>A nightly ingest that finishes silently is useless to operations; the counts
 * are stamped onto the {@code import_file} row (so a re-run can see the previous
 * outcome) and logged. A failed job is recorded as {@code FAILED}, not left as an
 * ambiguous half-state — a failed nightly file is an alert, not a gap to discover
 * later.
 */
public class ReconciliationReportListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationReportListener.class);

    private final JdbcTemplate jdbc;

    public ReconciliationReportListener(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String sourceFile = jobExecution.getJobParameters().getString("filePath");
        String sha256 = jobExecution.getJobParameters().getString("sha256");

        // A partitioned step reports BOTH a manager StepExecution (whose counts already aggregate
        // the workers) and one StepExecution per worker. Summing all of them would double-count, so
        // we sum only the worker executions (their names carry the ":partition-N" suffix); if the
        // job is not partitioned, there are no such names and we sum every step.
        boolean partitioned = jobExecution.getStepExecutions().stream()
                .anyMatch(s -> s.getStepName().contains(":partition"));

        long read = 0, written = 0, skipped = 0;
        for (StepExecution step : jobExecution.getStepExecutions()) {
            if (partitioned && !step.getStepName().contains(":partition")) {
                continue; // skip the manager step to avoid double-counting
            }
            read += step.getReadCount();
            written += step.getWriteCount();
            // process-skips + read-skips + write-skips = every quarantined row
            skipped += step.getProcessSkipCount() + step.getReadSkipCount() + step.getWriteSkipCount();
        }

        boolean ok = jobExecution.getStatus() == BatchStatus.COMPLETED;
        String status = ok ? "COMPLETED" : "FAILED";

        jdbc.update("""
                UPDATE import_file
                   SET status = ?, rows_read = ?, rows_imported = ?, rows_skipped = ?, finished_at = now()
                 WHERE sha256 = ?
                """, status, read, written, skipped, sha256);

        long durationMs = (jobExecution.getStartTime() == null) ? -1
                : java.time.Duration.between(jobExecution.getStartTime(), java.time.LocalDateTime.now()).toMillis();
        long rowsPerSec = durationMs > 0 ? (read * 1000L / durationMs) : -1;

        log.info("Reconciliation report for {}: status={} read={} imported={} skipped={} durationMs={} rowsPerSec={}",
                sourceFile, status, read, written, skipped, durationMs, rowsPerSec);

        if (skipped > 0) {
            List<Map<String, Object>> byReason = jdbc.queryForList("""
                    SELECT reason, count(*) AS n
                      FROM rejected_row
                     WHERE source_file = ?
                     GROUP BY reason
                     ORDER BY n DESC
                    """, sourceFile);
            byReason.forEach(r -> log.info("  rejected [{}]: {}", r.get("reason"), r.get("n")));
        }
    }
}
