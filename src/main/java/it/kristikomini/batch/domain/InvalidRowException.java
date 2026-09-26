package it.kristikomini.batch.domain;

/**
 * Thrown by the validating {@code ItemProcessor} for a row that is a <b>data</b>
 * problem — a single bad line the job should <i>skip</i> (bounded) and route to
 * the dead-letter table, not a reason to fail the whole run.
 *
 * <p>This is the deliberate counterpart to a fatal condition (missing mandatory
 * column, unreadable file): distinguishing "one bad row" from "the file/contract
 * is broken" is the whole point of the skip-vs-fail policy, and conflating them
 * is the classic batch bug (one malformed line aborts a 5M-row nightly job).
 *
 * <p>Carries the offending line number and a machine-usable {@code reason} code
 * so the dead-letter listener can write a per-reason reconciliation breakdown.
 */
public class InvalidRowException extends RuntimeException {

    private final long lineNumber;
    private final String reason;

    public InvalidRowException(long lineNumber, String reason, String detail) {
        super("line " + lineNumber + " [" + reason + "]: " + detail);
        this.lineNumber = lineNumber;
        this.reason = reason;
    }

    public long lineNumber() { return lineNumber; }

    public String reason() { return reason; }
}
