package ch.ffhs.bdn.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Partial aggregate for AVG: (sum, count).
 *
 * AVG itself is NOT associative (avg of avgs != avg), but (sum, count) is.
 * That is what makes a Combiner possible: partial sums/counts can be merged
 * on the map side and the division happens exactly once in the reducer.
 */
public class SumCountWritable implements Writable {

    private double sum;
    private long count;

    public SumCountWritable() { }

    public SumCountWritable(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public void set(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum()  { return sum; }
    public long getCount()  { return count; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readDouble();
        count = in.readLong();
    }

    @Override
    public String toString() {
        return sum + "\t" + count;
    }
}
