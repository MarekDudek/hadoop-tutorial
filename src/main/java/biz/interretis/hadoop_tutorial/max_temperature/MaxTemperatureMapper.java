package biz.interretis.hadoop_tutorial.max_temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {

        final String line = value.toString();

        final String year = line.substring(15, 19);
        final int airTemperature = Integer.parseInt(airTemperature(line));
        final String quality = line.substring(92, 93);

        if (airTemperature != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }

    private String airTemperature(final String line) {

        if (line.charAt(87) == '+') {
            return line.substring(88, 92);
        } else {
            return line.substring(87, 92);
        }
    }
}
