package ch.ffhs.bdn.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Aufgabe b) — variant 1: REDUCE-SIDE JOIN (repartition join)
 *
 *   SELECT bezeichnung, AVG(umsatz)
 *   FROM bigfile, bezeichnungen
 *   WHERE bigfile.land = bezeichnungen.land
 *   GROUP BY bezeichnung ORDER BY 2;
 *
 * DAG:
 *   [bigfile]       -> map S --\
 *                               >- Job B1 (join on land, partial sum/count) -> Job B2 (group by bezeichnung, avg) -> Job B3 (sort)
 *   [bezeichnungen] -> map B --/
 *
 * Why two aggregation steps? The shuffle key of the join is "land", but the
 * GROUP BY key is "bezeichnung". They are only identical if the mapping is 1:1.
 * If two lands share a bezeichnung (e.g. GB and UK -> "Vereinigtes Koenigreich"),
 * a second shuffle on bezeichnung is required. Because B1 emits (sum, count)
 * instead of an average, B2 still computes the exact SQL result.
 */
public final class JoinAvgByBezeichnungJob {

    private JoinAvgByBezeichnungJob() { }

    static final String TAG_SALES = "S";
    static final String TAG_BEZ   = "B";

    /** bigfile line -> (land, "S;umsatz;1") */
    public static class SalesTagMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text land = new Text();
        private final Text val = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            String s = line.toString().trim();
            if (s.isEmpty()) return;
            if (offset.get() == 0 && Common.isHeader(s)) {
                ctx.getCounter(Common.Records.HEADER).increment(1);
                return;
            }
            String[] f = s.split(Common.SEP, -1);
            try {
                String l = f[Common.COL_LAND].trim();
                String u = f[Common.COL_UMSATZ].trim();
                if (l.isEmpty() || u.isEmpty()) throw new IllegalArgumentException();
                Double.parseDouble(u);                       // validate
                land.set(l);
                val.set(TAG_SALES + Common.SEP + u + Common.SEP + "1");
                ctx.write(land, val);
                ctx.getCounter(Common.Records.VALID).increment(1);
            } catch (RuntimeException e) {
                ctx.getCounter(Common.Records.MALFORMED).increment(1);
            }
        }
    }

    /** bezeichnungen line "land;bezeichnung" -> (land, "B;bezeichnung") */
    public static class BezTagMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text land = new Text();
        private final Text val = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            String s = line.toString().trim();
            if (s.isEmpty() || (offset.get() == 0 && Common.isHeader(s))) return;
            String[] f = s.split(Common.SEP, 2);
            if (f.length < 2) {
                ctx.getCounter(Common.Records.MALFORMED).increment(1);
                return;
            }
            land.set(f[0].trim());
            val.set(TAG_BEZ + Common.SEP + f[1].trim());
            ctx.write(land, val);
        }
    }

    /**
     * Join-aware combiner: pre-aggregates the "S" records of a land on the map
     * side (massively reduces shuffle volume), passes "B" records through unchanged.
     */
    public static class JoinCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text out = new Text();

        @Override
        protected void reduce(Text land, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;
            for (Text v : values) {
                String[] f = v.toString().split(Common.SEP, 3);
                if (TAG_SALES.equals(f[0])) {
                    sum += Double.parseDouble(f[1]);
                    count += Long.parseLong(f[2]);
                } else {
                    ctx.write(land, v);                       // pass B through
                }
            }
            if (count > 0) {
                out.set(TAG_SALES + Common.SEP + sum + Common.SEP + count);
                ctx.write(land, out);
            }
        }
    }

    /**
     * Join reducer, called once per land with all S and B records of that land.
     * Inner join semantics: emit only if both sides exist.
     * Output line: "bezeichnung \t sum \t count"
     */
    public static class JoinReducer extends Reducer<Text, Text, Text, SumCountWritable> {
        private final Text bezKey = new Text();
        private final SumCountWritable sc = new SumCountWritable();

        @Override
        protected void reduce(Text land, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            List<String> bezeichnungen = new ArrayList<>();   // small: normally exactly 1
            double sum = 0;
            long count = 0;
            for (Text v : values) {
                String[] f = v.toString().split(Common.SEP, 3);
                if (TAG_SALES.equals(f[0])) {
                    sum += Double.parseDouble(f[1]);
                    count += Long.parseLong(f[2]);
                } else {
                    bezeichnungen.add(f[1]);
                }
            }
            if (bezeichnungen.isEmpty()) {
                ctx.getCounter(Common.Records.LANDS_WITHOUT_BEZEICHNUNG).increment(1);
                return;
            }
            if (count == 0) {
                ctx.getCounter(Common.Records.BEZEICHNUNG_WITHOUT_SALES).increment(1);
                return;
            }
            ctx.getCounter(Common.Records.JOINED_LANDS).increment(1);
            sc.set(sum, count);
            // 1:n mapping -> every matching bezeichnung gets the rows (same as SQL)
            for (String b : bezeichnungen) {
                bezKey.set(b);
                ctx.write(bezKey, sc);
            }
        }
    }

    /** B1 output line "bezeichnung \t sum \t count" -> (bezeichnung, (sum, count)) */
    public static class RegroupMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {
        private final Text bez = new Text();
        private final SumCountWritable sc = new SumCountWritable();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            if (f.length != 3) return;
            bez.set(f[0]);
            sc.set(Double.parseDouble(f[1]), Long.parseLong(f[2]));
            ctx.write(bez, sc);
        }
    }

    public static int run(Configuration conf, Path bigfile, Path bezeichnungen, Path out) throws Exception {
        Path step1 = new Path(out, "b1_join");
        Path step2 = new Path(out, "b2_avg_by_bezeichnung");
        Path step3 = new Path(out, "b3_sorted");

        // ---- B1: reduce-side join on land ----
        Job b1 = Job.getInstance(conf, "B1: JOIN bigfile x bezeichnungen ON land");
        b1.setJarByClass(JoinAvgByBezeichnungJob.class);
        MultipleInputs.addInputPath(b1, bigfile, TextInputFormat.class, SalesTagMapper.class);
        MultipleInputs.addInputPath(b1, bezeichnungen, TextInputFormat.class, BezTagMapper.class);
        b1.setCombinerClass(JoinCombiner.class);
        b1.setReducerClass(JoinReducer.class);
        b1.setMapOutputKeyClass(Text.class);
        b1.setMapOutputValueClass(Text.class);
        b1.setOutputKeyClass(Text.class);
        b1.setOutputValueClass(SumCountWritable.class);
        b1.setNumReduceTasks(conf.getInt("demo.reducers", 2));
        FileOutputFormat.setOutputPath(b1, step1);
        if (!b1.waitForCompletion(true)) return 1;

        // ---- B2: GROUP BY bezeichnung, AVG ----
        Job b2 = Job.getInstance(conf, "B2: GROUP BY bezeichnung, AVG(umsatz)");
        b2.setJarByClass(JoinAvgByBezeichnungJob.class);
        b2.setMapperClass(RegroupMapper.class);
        b2.setCombinerClass(Common.SumCountCombiner.class);
        b2.setReducerClass(Common.AvgReducer.class);
        b2.setMapOutputKeyClass(Text.class);
        b2.setMapOutputValueClass(SumCountWritable.class);
        b2.setOutputKeyClass(Text.class);
        b2.setOutputValueClass(DoubleWritable.class);
        b2.setNumReduceTasks(conf.getInt("demo.reducers", 2));
        FileInputFormat.addInputPath(b2, step1);
        FileOutputFormat.setOutputPath(b2, step2);
        if (!b2.waitForCompletion(true)) return 2;

        // ---- B3: ORDER BY 2 ----
        Job b3 = SortByAvgJob.create(conf, "B3: ORDER BY avg", step2, step3);
        return b3.waitForCompletion(true) ? 0 : 3;
    }
}
