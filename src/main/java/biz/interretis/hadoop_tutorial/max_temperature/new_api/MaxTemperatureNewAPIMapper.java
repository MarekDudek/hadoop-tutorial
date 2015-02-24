package biz.interretis.hadoop_tutorial.max_temperature.new_api;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureNewAPIMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {

        final String line = value.toString();

        final String year = WeatherRecord.YEAR.stringValue(line);
        final int airTemperature = WeatherRecord.AIR_TEMPERATURE.integerValue(line);
        final String quality = WeatherRecord.QUALITY.stringValue(line);

        if (airTemperature != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }

    private enum WeatherRecord {

        YEAR(15, 4),
        AIR_TEMPERATURE(87, 5),
        QUALITY(92, 1);

        private final int start;
        private final int length;

        private WeatherRecord(final int start, final int length) {
            this.start = start;
            this.length = length;
        }

        public String stringValue(final String line) {
            return line.substring(start, start + length);
        }

        public int integerValue(final String line) {
            int digitsBegin;
            if (line.charAt(start) == '+') {
                digitsBegin = start + 1;
            } else {
                digitsBegin = start;
            }
            final String number = line.substring(digitsBegin, start + length);
            return Integer.parseInt(number);
        }
    }
}
