package ch.ffhs.bdn.mr;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Shared building blocks used by both jobs.
 *
 * Data assumptions (see README):
 *   bigfile.csv        id;datum;land;umsatz       e.g. 4711;2026-03-14;CH;1234.50
 *   bezeichnungen.csv  land;bezeichnung           e.g. CH;Schweiz
 *   Separator ';', UTF-8, optional header line, umsatz with '.' as decimal point.
 */
public final class Common {

    private Common() { }

    public static final String SEP = ";";

    // Column positions in bigfile.csv
    public static final int COL_LAND   = 2;
    public static final int COL_UMSATZ = 3;

    /** Counter group shown in the job output / JobHistory UI. */
    public enum Records { VALID, MALFORMED, HEADER, JOINED_LANDS, LANDS_WITHOUT_BEZEICHNUNG, BEZEICHNUNG_WITHOUT_SALES }

    /** Combiner: merges partial (sum, count) pairs for the same key. Runs 0..n times. */
    public static class SumCountCombiner<K> extends Reducer<K, SumCountWritable, K, SumCountWritable> {
        private final SumCountWritable result = new SumCountWritable();

        @Override
        protected void reduce(K key, Iterable<SumCountWritable> values, Context ctx)
                throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;
            for (SumCountWritable v : values) {
                sum += v.getSum();
                count += v.getCount();
            }
            result.set(sum, count);
            ctx.write(key, result);
        }
    }

    /** Reducer: merges all (sum, count) pairs of a key and emits key -> sum/count. */
    public static class AvgReducer extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {
        private final DoubleWritable avg = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<SumCountWritable> values, Context ctx)
                throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;
            for (SumCountWritable v : values) {
                sum += v.getSum();
                count += v.getCount();
            }
            if (count > 0) {
                avg.set(sum / count);
                ctx.write(key, avg);
            }
        }
    }

    public static String fmt(double d) {
        return String.format(Locale.ROOT, "%.2f", d);
    }

    public static boolean isHeader(String line) {
        String l = line.toLowerCase(Locale.ROOT);
        return l.startsWith("id;") || l.startsWith("land;");
    }
}
