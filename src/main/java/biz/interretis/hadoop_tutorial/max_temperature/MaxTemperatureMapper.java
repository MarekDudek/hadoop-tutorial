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
        final int airTemperature = airTemperature(line);
        final String quality = line.substring(92, 93);

        if (airTemperature != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }

    private int airTemperature(final String line) {

        String substring;
        if (line.charAt(87) == '+') {
            substring = line.substring(88, 92);
        } else {
            substring = line.substring(87, 92);
        }
        return Integer.parseInt(substring);
    }
}
