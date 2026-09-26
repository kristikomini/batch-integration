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

        long read = 0, written = 0, skipped = 0;
        for (StepExecution step : jobExecution.getStepExecutions()) {
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

        log.info("Reconciliation report for {}: status={} read={} imported={} skipped={}",
                sourceFile, status, read, written, skipped);

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
