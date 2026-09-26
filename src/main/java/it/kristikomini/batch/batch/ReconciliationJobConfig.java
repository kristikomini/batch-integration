package it.kristikomini.batch.batch;

import it.kristikomini.batch.domain.InvalidRowException;
import it.kristikomini.batch.domain.RawRow;
import it.kristikomini.batch.domain.TransactionRecord;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.nio.file.Path;

/**
 * Wires the reconciliation job: a <b>partitioned</b> chunk step that streams the
 * file across worker threads, validates each row, and upserts it in JDBC batches.
 *
 * <p>The design decisions that make this production-grade rather than a {@code main()}
 * loop are all here:
 * <ul>
 *   <li><b>Partitioned</b> — {@link RangePartitioner} splits the file into line ranges;
 *       the master step fans them out to a bounded thread pool.</li>
 *   <li><b>Streaming reader</b> — {@link NioRangeItemReader} holds one line at a time,
 *       so heap is flat at any file size.</li>
 *   <li><b>JDBC-batch writer</b> — batched {@code INSERT … ON CONFLICT DO NOTHING}:
 *       the ~100× speedup over per-row JPA, and idempotent on the natural key.</li>
 *   <li><b>Fault tolerance by cause</b> — a bad row is <i>skipped</i> (bounded) to the
 *       dead-letter table; a transient DB error is <i>retried</i> (bounded); anything
 *       else fails the job.</li>
 *   <li><b>Restartable</b> — chunk commits persist reader position in the JobRepository,
 *       so a crashed run resumes from the last committed chunk.</li>
 * </ul>
 */
@Configuration
public class ReconciliationJobConfig {

    @Value("${reconciliation.grid-size:4}")
    private int gridSize;

    @Value("${reconciliation.chunk-size:1000}")
    private int chunkSize;

    @Value("${reconciliation.skip-limit:10000}")
    private int skipLimit;

    @Value("${reconciliation.retry-limit:3}")
    private int retryLimit;

    @Value("${reconciliation.has-header:true}")
    private boolean hasHeader;

    // ---- Job & steps ------------------------------------------------------

    @Bean
    public Job reconciliationJob(JobRepository jobRepository, Step masterStep,
                                 ReconciliationReportListener reportListener) {
        return new JobBuilder("reconciliationJob", jobRepository)
                .listener(reportListener)
                .start(masterStep)
                .build();
    }

    /**
     * The master step: it owns the {@link Partitioner} and hands each partition to a
     * worker-step execution on the thread pool. It does no I/O itself.
     */
    @Bean
    public Step masterStep(JobRepository jobRepository, Step workerStep,
                           Partitioner rangePartitioner, ThreadPoolTaskExecutor partitionTaskExecutor) {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("workerStep", rangePartitioner)
                .step(workerStep)
                .gridSize(gridSize)
                .taskExecutor(partitionTaskExecutor)
                .build();
    }

    @Bean
    public Step workerStep(JobRepository jobRepository, PlatformTransactionManager txManager,
                           NioRangeItemReader reader, TransactionValidator processor,
                           JdbcBatchItemWriter<TransactionRecord> writer,
                           DeadLetterSkipListener deadLetterListener) {
        return new StepBuilder("workerStep", jobRepository)
                .<RawRow, TransactionRecord>chunk(chunkSize, txManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                // A malformed row is one row's problem: skip it (bounded) and quarantine it.
                .faultTolerant()
                .skip(InvalidRowException.class)
                .skipLimit(skipLimit)
                // A deadlock / lock timeout is transient: retry it (bounded) before giving up.
                .retry(TransientDataAccessException.class)
                .retryLimit(retryLimit)
                .listener(deadLetterListener)
                .build();
    }

    // ---- Step-scoped components (per partition / per job parameters) ------

    @Bean
    @StepScope
    public Partitioner rangePartitioner(@Value("#{jobParameters['filePath']}") String filePath) {
        return new RangePartitioner(Path.of(filePath), hasHeader);
    }

    @Bean
    @StepScope
    public NioRangeItemReader reader(@Value("#{jobParameters['filePath']}") String filePath,
                                     @Value("#{stepExecutionContext['minLine']}") long minLine,
                                     @Value("#{stepExecutionContext['maxLine']}") long maxLine) {
        return new NioRangeItemReader(Path.of(filePath), hasHeader, minLine, maxLine);
    }

    @Bean
    @StepScope
    public TransactionValidator processor(@Value("#{jobParameters['filePath']}") String filePath) {
        return new TransactionValidator(filePath);
    }

    @Bean
    @StepScope
    public DeadLetterSkipListener deadLetterListener(JdbcTemplate jdbc,
                                                     @Value("#{jobParameters['filePath']}") String filePath) {
        return new DeadLetterSkipListener(jdbc, filePath);
    }

    // ---- Singletons -------------------------------------------------------

    /**
     * Batched upsert on the natural key. {@code assertUpdates(false)} is mandatory:
     * {@code ON CONFLICT DO NOTHING} legitimately affects 0 rows for a duplicate, and
     * the default would treat that as a failed write.
     */
    @Bean
    public JdbcBatchItemWriter<TransactionRecord> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<TransactionRecord>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO transaction_record
                            (txn_id, account, amount, currency, value_date, counterparty, source_file)
                        VALUES
                            (:txnId, :account, :amount, :currency, :valueDate, :counterparty, :sourceFile)
                        ON CONFLICT (txn_id) DO NOTHING
                        """)
                .beanMapped()
                .assertUpdates(false)
                .build();
    }

    @Bean
    public ReconciliationReportListener reportListener(JdbcTemplate jdbc) {
        return new ReconciliationReportListener(jdbc);
    }

    /** Bounded pool: one thread per partition, no more — the DB write path is the real ceiling. */
    @Bean
    public ThreadPoolTaskExecutor partitionTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(gridSize);
        executor.setMaxPoolSize(gridSize);
        executor.setThreadNamePrefix("partition-");
        executor.initialize();
        return executor;
    }
}
