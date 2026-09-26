package it.kristikomini.batch.batch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.item.ExecutionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The partitioner must tile the data rows into contiguous, non-overlapping ranges
 * that cover every row exactly once — otherwise parallel workers would double-import
 * or drop rows. Pure unit test over a temp file.
 */
class RangePartitionerTest {

    @TempDir
    Path tmp;

    private Path fileWithRows(int dataRows) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("txn_id,account,amount,currency,value_date,counterparty"); // header
        for (int i = 0; i < dataRows; i++) {
            lines.add("TX" + i + ",ACC1,1.00,EUR,2026-01-01,CP1");
        }
        Path f = tmp.resolve("feed.csv");
        Files.write(f, lines);
        return f;
    }

    @Test
    void tilesRowsIntoContiguousNonOverlappingRanges() throws IOException {
        Path file = fileWithRows(10);
        RangePartitioner partitioner = new RangePartitioner(file, true);

        Map<String, ExecutionContext> parts = partitioner.partition(3);

        assertThat(parts).hasSize(3);
        assertContiguousCover(parts.values(), 10);
        // 10 rows / 3 partitions → sizes 4,3,3 (remainder spread one-per-partition, differ by <= 1)
        List<Long> sizes = parts.values().stream()
                .map(c -> c.getLong("maxLine") - c.getLong("minLine"))
                .sorted()
                .toList();
        assertThat(sizes).containsExactly(3L, 3L, 4L);
    }

    @Test
    void capsPartitionsToRowCount() throws IOException {
        Path file = fileWithRows(2);
        RangePartitioner partitioner = new RangePartitioner(file, true);

        Map<String, ExecutionContext> parts = partitioner.partition(8);

        assertThat(parts).hasSize(2); // never more partitions than rows
        assertContiguousCover(parts.values(), 2);
    }

    @Test
    void handlesEmptyFileAsOneEmptyPartition() throws IOException {
        Path file = fileWithRows(0);
        RangePartitioner partitioner = new RangePartitioner(file, true);

        Map<String, ExecutionContext> parts = partitioner.partition(4);

        assertThat(parts).hasSize(1);
        ExecutionContext only = parts.values().iterator().next();
        assertThat(only.getLong("minLine")).isZero();
        assertThat(only.getLong("maxLine")).isZero();
    }

    /** Every row index in [0, total) is covered by exactly one range, with no overlap. */
    private void assertContiguousCover(Collection<ExecutionContext> parts, long total) {
        List<long[]> ranges = parts.stream()
                .map(c -> new long[]{c.getLong("minLine"), c.getLong("maxLine")})
                .sorted((a, b) -> Long.compare(a[0], b[0]))
                .toList();
        long expectedStart = 0;
        for (long[] r : ranges) {
            assertThat(r[0]).as("range start is contiguous").isEqualTo(expectedStart);
            assertThat(r[1]).as("range is non-empty or end").isGreaterThanOrEqualTo(r[0]);
            expectedStart = r[1];
        }
        assertThat(expectedStart).as("ranges cover all rows").isEqualTo(total);
    }
}
