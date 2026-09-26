package it.kristikomini.batch.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.List;

/**
 * Ingests a file passed on the command line: {@code java -jar app.jar --file=/path/to/feed.csv}.
 *
 * <p>Kept intentionally thin — real deployments would replace this with the SFTP
 * poller (see {@code docker-compose.yml}) that drops files into a landing directory
 * and calls {@link FileIngestService#ingest}. When no {@code --file} is given the app
 * just starts and waits, so it can run behind the poller or be driven by tests.
 */
@Component
public class IngestRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(IngestRunner.class);

    private final FileIngestService ingestService;

    public IngestRunner(FileIngestService ingestService) {
        this.ingestService = ingestService;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<String> files = args.getOptionValues("file");
        if (files == null || files.isEmpty()) {
            log.info("No --file given; app is up and idle. Pass --file=/path/to/feed.csv to ingest.");
            return;
        }
        for (String f : files) {
            FileIngestService.Result result = ingestService.ingest(Path.of(f));
            log.info("Ingest of {} -> {}", f, result.outcome());
        }
    }
}
