package ch.ffhs.bdn.mr;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Locale;
import java.util.Random;

/**
 * Generates deterministic demo data (seed 42) on the LOCAL filesystem.
 *
 *   bigfile.csv        id;datum;land;umsatz
 *   bezeichnungen.csv  land;bezeichnung
 *
 * Built-in edge cases for the discussion:
 *   - skew: CH and DE produce far more rows than the others
 *   - GB and UK both map to "Vereinigtes Koenigreich"  -> GROUP BY bezeichnung != GROUP BY land
 *   - XX has sales but no bezeichnung                    -> dropped by the inner join
 *   - LI has a bezeichnung but no sales                  -> not in the result
 *   - ~0.1% malformed rows / empty umsatz               -> MALFORMED counter
 */
public final class DataGenerator {

    private DataGenerator() { }

    //                          land, weight, mean umsatz (CHF)
    private static final Object[][] LANDS = {
        {"CH", 30, 1450.0}, {"DE", 25, 980.0}, {"AT", 8, 870.0}, {"FR", 9, 1020.0},
        {"IT", 7, 760.0},   {"NL", 4, 1180.0}, {"BE", 3, 940.0}, {"ES", 4, 650.0},
        {"US", 5, 2100.0},  {"GB", 2, 1300.0}, {"UK", 1, 1250.0}, {"JP", 1, 1750.0},
        {"XX", 1, 500.0},
    };

    private static final String[][] BEZEICHNUNGEN = {
        {"CH", "Schweiz"}, {"DE", "Deutschland"}, {"AT", "Oesterreich"}, {"FR", "Frankreich"},
        {"IT", "Italien"}, {"NL", "Niederlande"}, {"BE", "Belgien"},     {"ES", "Spanien"},
        {"US", "Vereinigte Staaten"}, {"GB", "Vereinigtes Koenigreich"},
        {"UK", "Vereinigtes Koenigreich"}, {"JP", "Japan"}, {"LI", "Liechtenstein"},
    };

    public static void generate(Path dir, long rows) throws IOException {
        Files.createDirectories(dir);
        Random rnd = new Random(42);
        int totalWeight = 0;
        for (Object[] l : LANDS) totalWeight += (Integer) l[1];
        LocalDate start = LocalDate.of(2026, 1, 1);

        try (BufferedWriter w = Files.newBufferedWriter(dir.resolve("bigfile.csv"), StandardCharsets.UTF_8)) {
            w.write("id;datum;land;umsatz\n");
            for (long id = 1; id <= rows; id++) {
                int pick = rnd.nextInt(totalWeight);
                Object[] land = LANDS[0];
                for (Object[] l : LANDS) {
                    pick -= (Integer) l[1];
                    if (pick < 0) { land = l; break; }
                }
                String datum = start.plusDays(rnd.nextInt(365)).toString();
                if (rnd.nextInt(1000) == 0) {                       // ~0.1% dirty rows
                    w.write(rnd.nextBoolean()
                            ? id + ";" + datum + ";" + land[0] + ";\n"           // empty umsatz (NULL)
                            : id + ";" + datum + ";" + land[0] + ";n/a\n");      // not a number
                    continue;
                }
                double mean = (Double) land[2];
                double umsatz = Math.max(1.0, mean * Math.exp(0.5 * rnd.nextGaussian() - 0.125));
                w.write(id + ";" + datum + ";" + land[0] + ";" + String.format(Locale.ROOT, "%.2f", umsatz) + "\n");
            }
        }

        try (BufferedWriter w = Files.newBufferedWriter(dir.resolve("bezeichnungen.csv"), StandardCharsets.UTF_8)) {
            w.write("land;bezeichnung\n");
            for (String[] b : BEZEICHNUNGEN) w.write(b[0] + ";" + b[1] + "\n");
        }
        System.out.println("Generated " + rows + " rows in " + dir.toAbsolutePath());
    }

    public static void main(String[] args) throws IOException {
        Path dir = Paths.get(args.length > 0 ? args[0] : "data");
        long rows = args.length > 1 ? Long.parseLong(args[1]) : 1_000_000L;
        generate(dir, rows);
    }
}
