package it.kristikomini.batch.batch;

import it.kristikomini.batch.domain.RawRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.item.ExecutionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reader must return exactly its slice {@code [minLine, maxLine)} of data rows,
 * skip the header, and report correct physical line numbers for dead-lettering.
 */
class NioRangeItemReaderTest {

    @TempDir
    Path tmp;

    private Path file(int dataRows) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("txn_id,account,amount,currency,value_date,counterparty");
        for (int i = 0; i < dataRows; i++) {
            lines.add("TX" + i);
        }
        Path f = tmp.resolve("feed.csv");
        Files.write(f, lines);
        return f;
    }

    private List<RawRow> readSlice(Path file, long min, long max) throws Exception {
        NioRangeItemReader reader = new NioRangeItemReader(file, true, min, max);
        List<RawRow> out = new ArrayList<>();
        reader.open(new ExecutionContext());
        try {
            RawRow r;
            while ((r = reader.read()) != null) {
                out.add(r);
            }
        } finally {
            reader.close();
        }
        return out;
    }

    @Test
    void readsOnlyItsSliceAndSkipsHeader() throws Exception {
        Path file = file(10);

        List<RawRow> slice = readSlice(file, 4, 7); // data indices 4,5,6

        assertThat(slice).extracting(RawRow::rawLine).containsExactly("TX4", "TX5", "TX6");
    }

    @Test
    void reportsPhysicalLineNumbers() throws Exception {
        Path file = file(10);

        List<RawRow> slice = readSlice(file, 0, 2); // first two data rows

        // header is file line 1, so data index 0 -> line 2, index 1 -> line 3
        assertThat(slice).extracting(RawRow::lineNumber).containsExactly(2L, 3L);
    }

    @Test
    void stopsAtEndOfFileEvenIfSliceReachesFurther() throws Exception {
        Path file = file(5);

        List<RawRow> slice = readSlice(file, 3, 100); // slice runs past EOF

        assertThat(slice).extracting(RawRow::rawLine).containsExactly("TX3", "TX4");
    }
}
