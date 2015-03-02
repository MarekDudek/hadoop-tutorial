package biz.interretis.hadoop_tutorial.max_temperature.old_api;

import biz.interretis.hadoop_tutorial.max_temperature.WeatherRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MaxTemperatureOldAPIMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(final Object key, final Text value, final OutputCollector<Text, IntWritable> output, final Reporter reporter) throws IOException {

        final String line = value.toString();

        final String year = WeatherRecord.YEAR.stringValue(line);
        final int airTemperature = WeatherRecord.AIR_TEMPERATURE.integerValue(line);
        final String quality = WeatherRecord.QUALITY.stringValue(line);


        if (airTemperature != WeatherRecord.MISSING && quality.matches("[01459]")) {
            output.collect(new Text(year), new IntWritable(airTemperature));
        }
    }
}
