package it.kristikomini.batch.batch;

import it.kristikomini.batch.domain.InvalidRowException;
import it.kristikomini.batch.domain.RawRow;
import it.kristikomini.batch.domain.TransactionRecord;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * The {@code ItemProcessor}: parses and validates one raw CSV line into a typed
 * {@link TransactionRecord}. Every rejection throws {@link InvalidRowException}
 * with a specific reason code, so the step's skip policy can quarantine the row
 * and the reconciliation report can break down rejections by cause.
 *
 * <p>Expected column layout (comma-separated, no quoting — a real supplier feed):
 * <pre>txn_id,account,amount,currency,value_date,counterparty</pre>
 *
 * <p>Pure and stateless, so it is safe to share across the partition worker
 * threads without synchronisation.
 */
public class TransactionValidator implements ItemProcessor<RawRow, TransactionRecord> {

    /** Accepted ISO-4217 currencies for this feed; anything else is a data error. */
    private static final Set<String> ALLOWED_CURRENCIES = Set.of("EUR", "USD", "GBP");
    private static final int EXPECTED_COLUMNS = 6;

    private final String sourceFile;

    public TransactionValidator(String sourceFile) {
        this.sourceFile = sourceFile;
    }

    @Override
    public TransactionRecord process(RawRow row) {
        long line = row.lineNumber();
        // -1 keeps trailing empty fields so a missing final column is caught, not silently dropped.
        String[] c = row.rawLine().split(",", -1);
        if (c.length != EXPECTED_COLUMNS) {
            throw new InvalidRowException(line, "COLUMN_COUNT",
                    "expected " + EXPECTED_COLUMNS + " columns, got " + c.length);
        }

        String txnId = c[0].trim();
        if (txnId.isEmpty()) {
            throw new InvalidRowException(line, "MISSING_TXN_ID", "natural key is blank");
        }

        String account = c[1].trim();
        if (account.isEmpty()) {
            throw new InvalidRowException(line, "MISSING_ACCOUNT", "account is mandatory");
        }

        BigDecimal amount;
        try {
            amount = new BigDecimal(c[2].trim());
        } catch (NumberFormatException e) {
            throw new InvalidRowException(line, "BAD_AMOUNT", "not a number: '" + c[2] + "'");
        }
        if (amount.signum() <= 0) {
            throw new InvalidRowException(line, "NON_POSITIVE_AMOUNT", "amount must be > 0: " + amount);
        }

        String currency = c[3].trim().toUpperCase();
        if (!ALLOWED_CURRENCIES.contains(currency)) {
            throw new InvalidRowException(line, "BAD_CURRENCY", "unsupported currency: '" + currency + "'");
        }

        LocalDate valueDate;
        try {
            valueDate = LocalDate.parse(c[4].trim());
        } catch (DateTimeParseException e) {
            throw new InvalidRowException(line, "BAD_DATE", "not an ISO date: '" + c[4] + "'");
        }

        String counterparty = c[5].trim();
        if (counterparty.isEmpty()) {
            throw new InvalidRowException(line, "MISSING_COUNTERPARTY", "counterparty is mandatory");
        }

        return new TransactionRecord(txnId, account, amount, currency, valueDate, counterparty, sourceFile);
    }
}
