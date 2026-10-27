package ch.ffhs.bdn.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * Aufgabe b) — variant 2: MAP-SIDE JOIN (broadcast / replicated join)
 *
 * bezeichnungen is tiny (one row per country) -> ship it to every mapper via
 * the Distributed Cache and join in memory. No shuffle needed for the join,
 * the mapper can emit the GROUP BY key (bezeichnung) directly.
 *
 * DAG:  [bigfile] + (cache: bezeichnungen) -> Job M1 (map: lookup+parse, combine, reduce: avg) -> Job M2 (sort)
 *
 * Same result as the reduce-side join, one job less, far less shuffle data.
 */
public final class MapJoinAvgByBezeichnungJob {

    private MapJoinAvgByBezeichnungJob() { }

    static final String CACHE_LINK = "bezeichnungen.lookup";

    public static class LookupMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {
        private final Map<String, List<String>> lookup = new HashMap<>();
        private final Text bez = new Text();
        private final SumCountWritable one = new SumCountWritable();

        @Override
        protected void setup(Context ctx) throws IOException {
            // The cache file is symlinked into the task's working dir under CACHE_LINK
            try (BufferedReader r = new BufferedReader(new InputStreamReader(
                    Files.newInputStream(Paths.get(CACHE_LINK)), StandardCharsets.UTF_8))) {
                String line;
                boolean first = true;
                while ((line = r.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || (first && Common.isHeader(line))) { first = false; continue; }
                    first = false;
                    String[] f = line.split(Common.SEP, 2);
                    if (f.length == 2) {
                        lookup.computeIfAbsent(f[0].trim(), k -> new ArrayList<>()).add(f[1].trim());
                    }
                }
            }
        }

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
            double umsatz;
            String land;
            try {
                land = f[Common.COL_LAND].trim();
                String u = f[Common.COL_UMSATZ].trim();
                if (land.isEmpty() || u.isEmpty()) throw new IllegalArgumentException();
                umsatz = Double.parseDouble(u);
            } catch (RuntimeException e) {
                ctx.getCounter(Common.Records.MALFORMED).increment(1);
                return;
            }
            List<String> matches = lookup.get(land);
            if (matches == null) {                       // inner join: drop
                ctx.getCounter(Common.Records.LANDS_WITHOUT_BEZEICHNUNG).increment(1);
                return;
            }
            ctx.getCounter(Common.Records.VALID).increment(1);
            one.set(umsatz, 1);
            for (String b : matches) {
                bez.set(b);
                ctx.write(bez, one);
            }
        }
    }

    public static int run(Configuration conf, Path bigfile, Path bezeichnungen, Path out) throws Exception {
        Path step1 = new Path(out, "m1_mapjoin_avg");
        Path step2 = new Path(out, "m2_sorted");

        Job m1 = Job.getInstance(conf, "M1: MAP-SIDE JOIN + GROUP BY bezeichnung, AVG(umsatz)");
        m1.setJarByClass(MapJoinAvgByBezeichnungJob.class);
        URI cache = bezeichnungen.getFileSystem(conf).makeQualified(bezeichnungen).toUri();
        m1.addCacheFile(new URI(cache.toString() + "#" + CACHE_LINK));
        m1.setMapperClass(LookupMapper.class);
        m1.setCombinerClass(Common.SumCountCombiner.class);
        m1.setReducerClass(Common.AvgReducer.class);
        m1.setMapOutputKeyClass(Text.class);
        m1.setMapOutputValueClass(SumCountWritable.class);
        m1.setOutputKeyClass(Text.class);
        m1.setOutputValueClass(DoubleWritable.class);
        m1.setNumReduceTasks(conf.getInt("demo.reducers", 2));
        FileInputFormat.addInputPath(m1, bigfile);
        FileOutputFormat.setOutputPath(m1, step1);
        if (!m1.waitForCompletion(true)) return 1;

        Job m2 = SortByAvgJob.create(conf, "M2: ORDER BY avg", step1, step2);
        return m2.waitForCompletion(true) ? 0 : 2;
    }
}
