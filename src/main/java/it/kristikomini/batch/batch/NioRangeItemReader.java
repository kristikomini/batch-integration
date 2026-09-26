package it.kristikomini.batch.batch;

import it.kristikomini.batch.domain.RawRow;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Streams one partition's slice of the file — data rows {@code [minLine, maxLine)}
 * — using a buffered {@code java.nio} reader. It holds <b>one line at a time</b> in
 * memory, so heap stays flat whether the file is 5 thousand rows or 5 million.
 *
 * <p>Extends {@link AbstractItemCountingItemStreamItemReader}, which persists the
 * read count into the step's {@code ExecutionContext} at each chunk commit. That is
 * what makes the job <b>restartable</b>: after a crash, this reader reopens and
 * fast-forwards to the last committed item, so a run that died at row 3.4M resumes
 * there instead of re-reading (and the writer's upsert makes any small overlap a
 * no-op anyway).
 *
 * <p>Step-scoped: one instance per partition, each with its own {@code minLine}/
 * {@code maxLine} injected from the partition {@code ExecutionContext}.
 */
public class NioRangeItemReader extends AbstractItemCountingItemStreamItemReader<RawRow> {

    private final Path file;
    private final boolean hasHeader;
    private final long minLine;   // inclusive, 0-based data-row index
    private final long maxLine;   // exclusive

    private BufferedReader reader;
    private long nextDataIndex;   // 0-based data index of the row the next doRead() will return

    public NioRangeItemReader(Path file, boolean hasHeader, long minLine, long maxLine) {
        this.file = file;
        this.hasHeader = hasHeader;
        this.minLine = minLine;
        this.maxLine = maxLine;
        // Distinct per partition so restart state does not collide in the ExecutionContext.
        setName("nioRangeReader-" + minLine + "-" + maxLine);
        // Bound this reader to its slice; the parent returns null once the slice is exhausted.
        setMaxItemCount((int) Math.min(Integer.MAX_VALUE, maxLine - minLine));
    }

    @Override
    protected void doOpen() {
        try {
            reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
            if (hasHeader) {
                reader.readLine(); // discard the header row
            }
            // Skip the data rows that belong to earlier partitions.
            for (long i = 0; i < minLine; i++) {
                if (reader.readLine() == null) {
                    break;
                }
            }
            nextDataIndex = minLine;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open file slice: " + file, e);
        }
    }

    @Override
    protected RawRow doRead() {
        try {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            // Physical (1-based) line number for dead-letter reporting: header is line 1, so
            // data-row index d sits at file line d + (hasHeader ? 2 : 1).
            long physicalLine = nextDataIndex + (hasHeader ? 2 : 1);
            nextDataIndex++;
            return new RawRow(physicalLine, line);
        } catch (IOException e) {
            throw new UncheckedIOException("read error in file slice: " + file, e);
        }
    }

    @Override
    protected void doClose() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("close error: " + file, e);
        } finally {
            reader = null;
        }
    }
}
