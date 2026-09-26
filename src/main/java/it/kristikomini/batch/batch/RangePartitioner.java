package it.kristikomini.batch.batch;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Splits the data rows of the file into {@code gridSize} contiguous line ranges,
 * one per partition, so the worker step can run them on separate threads.
 *
 * <p>Ranges are <b>half-open</b> {@code [minLine, maxLine)} over 0-based data-row
 * indices (the header is excluded). Because the ranges tile the row space without
 * gaps or overlap, every row is processed exactly once even under parallelism.
 *
 * <p>Splitting by <i>line range</i> (rather than raw byte offset) costs one cheap
 * streaming pass to count lines, but keeps each worker's reader trivially correct:
 * it never has to resynchronise onto a line boundary mid-file. Counting streams the
 * file and never holds it in memory, so it is safe at 5M rows under a small heap.
 */
public class RangePartitioner implements Partitioner {

    private final Path file;
    private final boolean hasHeader;

    public RangePartitioner(Path file, boolean hasHeader) {
        this.file = file;
        this.hasHeader = hasHeader;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        long dataRows = countDataRows();
        Map<String, ExecutionContext> partitions = new HashMap<>();

        // Never create more partitions than there are rows (empty partitions are wasted threads).
        int grid = (int) Math.max(1, Math.min(gridSize, dataRows == 0 ? 1 : dataRows));
        long base = dataRows / grid;
        long remainder = dataRows % grid;

        long cursor = 0;
        for (int i = 0; i < grid; i++) {
            // Spread the remainder one row at a time across the first partitions, so sizes differ by at most 1.
            long size = base + (i < remainder ? 1 : 0);
            long min = cursor;
            long max = cursor + size;
            cursor = max;

            ExecutionContext ctx = new ExecutionContext();
            ctx.putString("filePath", file.toString());
            ctx.putLong("minLine", min);
            ctx.putLong("maxLine", max);
            ctx.putString("partition", "p" + i);
            partitions.put("partition-" + i, ctx);
        }
        return partitions;
    }

    private long countDataRows() {
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
             Stream<String> lines = reader.lines()) {
            long total = lines.count();
            return hasHeader ? Math.max(0, total - 1) : total;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read file for partitioning: " + file, e);
        }
    }
}
