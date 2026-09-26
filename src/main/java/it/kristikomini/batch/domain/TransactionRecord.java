package it.kristikomini.batch.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * A validated, type-safe transaction ready to be written. The {@code txnId} is
 * the <b>natural key</b> — the writer upserts on it ({@code ON CONFLICT DO NOTHING}),
 * which is what makes re-ingesting the same file a no-op instead of a duplicate.
 *
 * <p>Getters (not just record accessors) are provided because {@code JdbcBatchItemWriter}
 * with {@code beanMapped()} resolves named SQL parameters via JavaBean property names
 * ({@code getTxnId()} → {@code :txnId}); records expose {@code txnId()}, which the
 * bean property mapper does not recognise.
 */
public final class TransactionRecord {

    private final String txnId;
    private final String account;
    private final BigDecimal amount;
    private final String currency;
    private final LocalDate valueDate;
    private final String counterparty;
    private final String sourceFile;

    public TransactionRecord(String txnId, String account, BigDecimal amount, String currency,
                             LocalDate valueDate, String counterparty, String sourceFile) {
        this.txnId = txnId;
        this.account = account;
        this.amount = amount;
        this.currency = currency;
        this.valueDate = valueDate;
        this.counterparty = counterparty;
        this.sourceFile = sourceFile;
    }

    public String getTxnId() { return txnId; }
    public String getAccount() { return account; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public LocalDate getValueDate() { return valueDate; }
    public String getCounterparty() { return counterparty; }
    public String getSourceFile() { return sourceFile; }
}
