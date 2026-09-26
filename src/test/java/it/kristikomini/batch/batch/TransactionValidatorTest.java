package it.kristikomini.batch.batch;

import it.kristikomini.batch.domain.InvalidRowException;
import it.kristikomini.batch.domain.RawRow;
import it.kristikomini.batch.domain.TransactionRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The validator is where a raw line becomes a typed record — or is rejected with a
 * specific reason. These are pure unit tests (no Spring, no DB), so they run locally.
 */
class TransactionValidatorTest {

    private final TransactionValidator validator = new TransactionValidator("feed.csv");

    private static RawRow row(String line) {
        return new RawRow(2, line);
    }

    @Test
    void parsesAWellFormedRow() {
        TransactionRecord r = validator.process(row("TX1,ACC7,10.50,eur,2026-02-01,CP42"));

        assertThat(r.getTxnId()).isEqualTo("TX1");
        assertThat(r.getAccount()).isEqualTo("ACC7");
        assertThat(r.getAmount()).isEqualByComparingTo(new BigDecimal("10.50"));
        assertThat(r.getCurrency()).isEqualTo("EUR"); // normalised to upper-case
        assertThat(r.getValueDate()).isEqualTo(LocalDate.of(2026, 2, 1));
        assertThat(r.getCounterparty()).isEqualTo("CP42");
        assertThat(r.getSourceFile()).isEqualTo("feed.csv");
    }

    @Test
    void rejectsWrongColumnCount() {
        assertThatThrownBy(() -> validator.process(row("TX1,ACC7,10.50,EUR,2026-02-01")))
                .isInstanceOf(InvalidRowException.class)
                .satisfies(e -> assertThat(((InvalidRowException) e).reason()).isEqualTo("COLUMN_COUNT"));
    }

    @Test
    void rejectsBlankNaturalKey() {
        assertThatThrownBy(() -> validator.process(row(" ,ACC7,10.50,EUR,2026-02-01,CP42")))
                .isInstanceOf(InvalidRowException.class)
                .satisfies(e -> assertThat(((InvalidRowException) e).reason()).isEqualTo("MISSING_TXN_ID"));
    }

    @Test
    void rejectsNonNumericAmount() {
        assertThatThrownBy(() -> validator.process(row("TX1,ACC7,ten,EUR,2026-02-01,CP42")))
                .isInstanceOf(InvalidRowException.class)
                .satisfies(e -> assertThat(((InvalidRowException) e).reason()).isEqualTo("BAD_AMOUNT"));
    }

    @Test
    void rejectsNonPositiveAmount() {
        assertThatThrownBy(() -> validator.process(row("TX1,ACC7,-5.00,EUR,2026-02-01,CP42")))
                .isInstanceOf(InvalidRowException.class)
                .satisfies(e -> assertThat(((InvalidRowException) e).reason()).isEqualTo("NON_POSITIVE_AMOUNT"));
    }

    @Test
    void rejectsUnsupportedCurrency() {
        assertThatThrownBy(() -> validator.process(row("TX1,ACC7,10.50,XXX,2026-02-01,CP42")))
                .isInstanceOf(InvalidRowException.class)
                .satisfies(e -> assertThat(((InvalidRowException) e).reason()).isEqualTo("BAD_CURRENCY"));
    }

    @Test
    void rejectsNonIsoDate() {
        assertThatThrownBy(() -> validator.process(row("TX1,ACC7,10.50,EUR,01/02/2026,CP42")))
                .isInstanceOf(InvalidRowException.class)
                .satisfies(e -> assertThat(((InvalidRowException) e).reason()).isEqualTo("BAD_DATE"));
    }

    @Test
    void carriesTheLineNumberForDeadLettering() {
        assertThatThrownBy(() -> validator.process(new RawRow(4242, "TX1,ACC7,bad,EUR,2026-02-01,CP42")))
                .isInstanceOf(InvalidRowException.class)
                .satisfies(e -> assertThat(((InvalidRowException) e).lineNumber()).isEqualTo(4242));
    }
}
