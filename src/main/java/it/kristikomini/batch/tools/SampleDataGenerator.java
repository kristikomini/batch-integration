package it.kristikomini.batch.tools;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Random;

/**
 * Generates a deterministic sample feed for the benchmark — the same seed always
 * produces the same file, so the 5M-row runs are comparable across approaches.
 *
 * <p>Streams the rows out one at a time (never builds them in memory), so it can
 * write a 5M-row file in constant heap, and salts in a configurable fraction of
 * deliberately-malformed rows so the skip / dead-letter path is exercised, not just
 * the happy path.
 *
 * <pre>
 *   java -cp target/classes it.kristikomini.batch.tools.SampleDataGenerator out.csv 5000000 0.001
 *   #                                                                        file    rows     badFraction
 * </pre>
 */
public final class SampleDataGenerator {

    private static final String[] CURRENCIES = {"EUR", "USD", "GBP"};

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("usage: SampleDataGenerator <outFile> <rows> [badFraction] [seed]");
            System.exit(2);
        }
        Path out = Path.of(args[0]);
        long rows = Long.parseLong(args[1]);
        double badFraction = args.length > 2 ? Double.parseDouble(args[2]) : 0.001;
        long seed = args.length > 3 ? Long.parseLong(args[3]) : 42L;

        long written = write(out, rows, badFraction, seed);
        System.out.printf("Wrote %d rows to %s (bad ~%.3f%%)%n", written, out, badFraction * 100);
    }

    /** Writes {@code rows} data lines (plus header) and returns the count written. */
    public static long write(Path out, long rows, double badFraction, long seed) throws IOException {
        Random rnd = new Random(seed);
        LocalDate base = LocalDate.of(2026, 1, 1);
        try (BufferedWriter w = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            w.write("txn_id,account,amount,currency,value_date,counterparty");
            w.newLine();
            for (long i = 0; i < rows; i++) {
                if (rnd.nextDouble() < badFraction) {
                    w.write(malformed(i, rnd));
                } else {
                    String cur = CURRENCIES[rnd.nextInt(CURRENCIES.length)];
                    long amountCents = 1 + rnd.nextInt(1_000_000);
                    LocalDate date = base.plusDays(rnd.nextInt(365));
                    w.write("TX" + i + ",ACC" + (rnd.nextInt(100_000))
                            + "," + (amountCents / 100.0)
                            + "," + cur
                            + "," + date
                            + ",CP" + rnd.nextInt(5_000));
                }
                w.newLine();
            }
        }
        return rows;
    }

    /** A row that will trip one of the validator's rules, chosen at random. */
    private static String malformed(long i, Random rnd) {
        return switch (rnd.nextInt(4)) {
            case 0 -> "TX" + i + ",ACC1,-5.00,EUR,2026-02-01,CP1";      // non-positive amount
            case 1 -> "TX" + i + ",ACC1,10.00,XXX,2026-02-01,CP1";      // bad currency
            case 2 -> "TX" + i + ",ACC1,10.00,EUR,not-a-date,CP1";      // bad date
            default -> "TX" + i + ",ACC1,10.00,EUR,2026-02-01";        // missing column
        };
    }

    private SampleDataGenerator() {}
}
