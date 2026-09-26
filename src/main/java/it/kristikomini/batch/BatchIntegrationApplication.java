package it.kristikomini.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point. The batch job does NOT run at startup on its own — we disable
 * {@code spring.batch.job.enabled} and launch it explicitly through
 * {@link it.kristikomini.batch.ingest.FileIngestService}, so file discovery,
 * sha256 dedupe and the launch decision live in one place (and tests can drive
 * the job with controlled parameters).
 */
@SpringBootApplication
public class BatchIntegrationApplication {
    public static void main(String[] args) {
        SpringApplication.run(BatchIntegrationApplication.class, args);
    }
}
