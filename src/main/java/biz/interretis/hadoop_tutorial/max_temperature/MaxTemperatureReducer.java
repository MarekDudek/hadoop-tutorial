package biz.interretis.hadoop_tutorial.max_temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {

        int maxValue = Integer.MIN_VALUE;

        for (IntWritable value: values) {
            final int unpacked = value.get();
            maxValue = Math.max(maxValue, unpacked);
        }

        final IntWritable result = new IntWritable(maxValue);

        context.write(key, result);
    }
}
