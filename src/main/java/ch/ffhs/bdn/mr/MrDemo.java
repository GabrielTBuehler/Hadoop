package ch.ffhs.bdn.mr;

import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Entry point:
 *
 *   hadoop jar mr-demo.jar a          <bigfile> <outDir>
 *   hadoop jar mr-demo.jar b          <bigfile> <bezeichnungen> <outDir>   (reduce-side join)
 *   hadoop jar mr-demo.jar b-mapjoin  <bigfile> <bezeichnungen> <outDir>   (map-side join)
 *   java -cp mr-demo.jar ch.ffhs.bdn.mr.DataGenerator <localDir> <rows>
 *
 * Generic Hadoop options work via ToolRunner, e.g. -D demo.reducers=4
 * The outDir is deleted before the run (demo convenience).
 */
public class MrDemo extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) return usage();
        Configuration conf = getConf();

        switch (args[0]) {
            case "a": {
                if (args.length != 3) return usage();
                Path out = clean(conf, args[2]);
                return AvgByLandJob.run(conf, new Path(args[1]), out);
            }
            case "b": {
                if (args.length != 4) return usage();
                Path out = clean(conf, args[3]);
                return JoinAvgByBezeichnungJob.run(conf, new Path(args[1]), new Path(args[2]), out);
            }
            case "b-mapjoin": {
                if (args.length != 4) return usage();
                Path out = clean(conf, args[3]);
                return MapJoinAvgByBezeichnungJob.run(conf, new Path(args[1]), new Path(args[2]), out);
            }
            case "generate": {
                DataGenerator.generate(Paths.get(args.length > 1 ? args[1] : "data"),
                        args.length > 2 ? Long.parseLong(args[2]) : 1_000_000L);
                return 0;
            }
            default:
                return usage();
        }
    }

    private static Path clean(Configuration conf, String dir) throws Exception {
        Path p = new Path(dir);
        FileSystem fs = p.getFileSystem(conf);
        if (fs.exists(p)) fs.delete(p, true);
        return p;
    }

    private static int usage() {
        System.err.println("Usage:\n"
            + "  a          <bigfile> <outDir>\n"
            + "  b          <bigfile> <bezeichnungen> <outDir>\n"
            + "  b-mapjoin  <bigfile> <bezeichnungen> <outDir>\n"
            + "  generate   <localDir> <rows>");
        return 64;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new MrDemo(), args));
    }
}
