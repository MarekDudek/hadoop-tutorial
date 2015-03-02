package biz.interretis.hadoop_tutorial.max_temperature.old_api;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class MaxTemperatureOldAPIReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(final Text key, final Iterator<IntWritable> values, final OutputCollector<Text, IntWritable> collector, final Reporter reporter) throws IOException {

        int max = Integer.MIN_VALUE;

        while (values.hasNext()) {
            final IntWritable value = values.next();
            final int unpacked = value.get();
            max = Math.max(max, unpacked);
        }

        final IntWritable result = new IntWritable(max);
        collector.collect(key, result);
    }
}
