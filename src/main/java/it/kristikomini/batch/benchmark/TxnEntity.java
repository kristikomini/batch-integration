package it.kristikomini.batch.benchmark;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * JPA mapping of {@code transaction_record}, used ONLY by {@link BenchmarkRunner} to measure the
 * naive/JPA loading approaches against the streaming JDBC-batch writer the real job uses. The
 * production ingest path deliberately does not use JPA — this entity exists to quantify why.
 */
@Entity
@Table(name = "transaction_record")
public class TxnEntity {

    @Id
    @Column(name = "txn_id")
    private String txnId;

    private String account;
    private BigDecimal amount;

    // The column is CHAR(3); tell Hibernate so schema-validation matches (else it expects VARCHAR).
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(length = 3)
    private String currency;

    @Column(name = "value_date")
    private LocalDate valueDate;

    private String counterparty;

    @Column(name = "source_file")
    private String sourceFile;

    protected TxnEntity() {
    }

    public TxnEntity(String txnId, String account, BigDecimal amount, String currency,
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
}
