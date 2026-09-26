package it.kristikomini.batch.domain;

/**
 * A single physical line read from the file, paired with its 1-based line number
 * in the source (the header is line 1; the first data row is line 2).
 *
 * <p>The reader emits this "raw" item deliberately un-parsed: validation and
 * type conversion happen in the {@code ItemProcessor}, so that a malformed row
 * throws where the skip/dead-letter policy can catch it and record the offending
 * line — rather than blowing up inside the reader where Spring Batch cannot
 * attribute the failure to a specific item.
 */
public record RawRow(long lineNumber, String rawLine) {}
