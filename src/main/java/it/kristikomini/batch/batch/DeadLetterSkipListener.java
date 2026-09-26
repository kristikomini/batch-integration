package it.kristikomini.batch.batch;

import it.kristikomini.batch.domain.InvalidRowException;
import it.kristikomini.batch.domain.RawRow;
import it.kristikomini.batch.domain.TransactionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Routes skipped rows to the {@code rejected_row} dead-letter table instead of
 * losing them. A skipped row is not swept under the rug: the supplier gets a
 * per-reason quarantine of exactly which lines were rejected and why, which is the
 * difference between "we imported 4,999,records, here are the 812 we couldn't and
 * why" and a silent nightly gap nobody notices until reconciliation breaks.
 *
 * <p>The {@code reason} code comes straight from {@link InvalidRowException}, so the
 * reconciliation report can {@code GROUP BY reason} to show the breakdown by cause.
 */
public class DeadLetterSkipListener implements SkipListener<RawRow, TransactionRecord> {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterSkipListener.class);

    private final JdbcTemplate jdbc;
    private final String sourceFile;

    public DeadLetterSkipListener(JdbcTemplate jdbc, String sourceFile) {
        this.jdbc = jdbc;
        this.sourceFile = sourceFile;
    }

    /** A row rejected by the validating processor — the common case. */
    @Override
    public void onSkipInProcess(RawRow item, Throwable t) {
        String reason = (t instanceof InvalidRowException ir) ? ir.reason() : "PROCESS_ERROR";
        insert(item.lineNumber(), reason, item.rawLine(), t.getMessage());
    }

    /** A row that could not even be read (rare with a line reader; recorded for completeness). */
    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("skipped unreadable row in {}: {}", sourceFile, t.getMessage());
    }

    /** A row that validated but failed on write after retries were exhausted. */
    @Override
    public void onSkipInWrite(TransactionRecord item, Throwable t) {
        insert(-1, "WRITE_FAILED", item.getTxnId(), t.getMessage());
    }

    private void insert(long line, String reason, String raw, String error) {
        jdbc.update("""
                INSERT INTO rejected_row (source_file, line_number, reason, raw_line, error_detail)
                VALUES (?, ?, ?, ?, ?)
                """, sourceFile, line, reason, raw, error);
    }
}
