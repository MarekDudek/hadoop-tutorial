package biz.interretis.hadoop_tutorial.max_temperature.new_api;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureNewAPIReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {

        int max = Integer.MIN_VALUE;

        for (IntWritable value : values) {
            final int unpacked = value.get();
            max = Math.max(max, unpacked);
        }

        final IntWritable result = new IntWritable(max);
        context.write(key, result);
    }
}
