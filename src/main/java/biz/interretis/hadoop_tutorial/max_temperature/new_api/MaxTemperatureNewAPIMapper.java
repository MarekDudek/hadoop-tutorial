package biz.interretis.hadoop_tutorial.max_temperature.new_api;

import biz.interretis.hadoop_tutorial.max_temperature.WeatherRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureNewAPIMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {

        final String line = value.toString();

        final String year = WeatherRecord.YEAR.stringValue(line);
        final int airTemperature = WeatherRecord.AIR_TEMPERATURE.integerValue(line);
        final String quality = WeatherRecord.QUALITY.stringValue(line);

        if (airTemperature != WeatherRecord.MISSING && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
