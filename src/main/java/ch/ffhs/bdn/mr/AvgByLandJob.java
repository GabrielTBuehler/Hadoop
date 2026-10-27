package ch.ffhs.bdn.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Aufgabe a)
 *
 *   SELECT land, AVG(umsatz) FROM bigfile GROUP BY land ORDER BY 2;
 *
 * DAG:  [bigfile] -> Job A1 (map: parse, combine: sum/count, reduce: avg) -> Job A2 (sort) -> result
 */
public final class AvgByLandJob {

    private AvgByLandJob() { }

    /** map(offset, line) -> (land, (umsatz, 1)) */
    public static class SalesMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {
        private final Text land = new Text();
        private final SumCountWritable one = new SumCountWritable();

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
                // SQL AVG ignores NULLs -> empty umsatz is skipped (not counted)
                String u = f[Common.COL_UMSATZ].trim();
                String l = f[Common.COL_LAND].trim();
                if (u.isEmpty() || l.isEmpty()) throw new IllegalArgumentException();
                land.set(l);
                one.set(Double.parseDouble(u), 1);
                ctx.write(land, one);
                ctx.getCounter(Common.Records.VALID).increment(1);
            } catch (RuntimeException e) {
                ctx.getCounter(Common.Records.MALFORMED).increment(1);
            }
        }
    }

    /** Runs A1 then A2. Intermediate result: <out>/a1_avg_by_land, final: <out>/a2_sorted */
    public static int run(Configuration conf, Path bigfile, Path out) throws Exception {
        Path step1 = new Path(out, "a1_avg_by_land");
        Path step2 = new Path(out, "a2_sorted");

        Job a1 = Job.getInstance(conf, "A1: GROUP BY land, AVG(umsatz)");
        a1.setJarByClass(AvgByLandJob.class);
        a1.setMapperClass(SalesMapper.class);
        a1.setCombinerClass(Common.SumCountCombiner.class);
        a1.setReducerClass(Common.AvgReducer.class);
        a1.setMapOutputKeyClass(Text.class);
        a1.setMapOutputValueClass(SumCountWritable.class);
        a1.setOutputKeyClass(Text.class);
        a1.setOutputValueClass(DoubleWritable.class);
        a1.setNumReduceTasks(conf.getInt("demo.reducers", 2));   // >1 to show partitioning
        FileInputFormat.addInputPath(a1, bigfile);
        FileOutputFormat.setOutputPath(a1, step1);
        if (!a1.waitForCompletion(true)) return 1;

        Job a2 = SortByAvgJob.create(conf, "A2: ORDER BY avg", step1, step2);
        return a2.waitForCompletion(true) ? 0 : 2;
    }
}
