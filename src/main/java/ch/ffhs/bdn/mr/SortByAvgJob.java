package ch.ffhs.bdn.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * "ORDER BY 2" — global sort by the average.
 *
 * Trick: swap key and value. The shuffle phase sorts by key, so emitting
 * (avg, name) lets the framework do the sorting. With ONE reducer the result
 * is globally sorted (fine here: one row per country/bezeichnung).
 * For huge outputs you would use TotalOrderPartitioner + InputSampler instead.
 */
public final class SortByAvgJob {

    private SortByAvgJob() { }

    /** Input line: "<name>\t<avg>"  ->  (avg, name) */
    public static class SwapMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        private final DoubleWritable outKey = new DoubleWritable();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx)
                throws IOException, InterruptedException {
            String s = line.toString();
            int tab = s.lastIndexOf('\t');
            if (tab < 0) return;
            outVal.set(s.substring(0, tab));
            outKey.set(Double.parseDouble(s.substring(tab + 1)));
            ctx.write(outKey, outVal);
        }
    }

    /** Keys arrive sorted ascending; write "name \t avg" (ties: all names of that avg). */
    public static class EmitReducer extends Reducer<DoubleWritable, Text, Text, NullWritable> {
        private final Text out = new Text();

        @Override
        protected void reduce(DoubleWritable avg, Iterable<Text> names, Context ctx)
                throws IOException, InterruptedException {
            for (Text name : names) {
                out.set(name + "\t" + Common.fmt(avg.get()));
                ctx.write(out, NullWritable.get());
            }
        }
    }

    public static Job create(Configuration conf, String name, Path in, Path out) throws IOException {
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(SortByAvgJob.class);

        job.setMapperClass(SwapMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);                 // single reducer = global order
        job.setReducerClass(EmitReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
