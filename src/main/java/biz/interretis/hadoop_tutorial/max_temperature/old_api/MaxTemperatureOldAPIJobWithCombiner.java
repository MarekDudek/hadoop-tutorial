package biz.interretis.hadoop_tutorial.max_temperature.old_api;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class MaxTemperatureOldAPIJobWithCombiner {

    public static void main(final String ...args) throws IOException {

        final JobConf config = new JobConf(MaxTemperatureOldAPIJobWithCombiner.class);
        config.setJobName("Max temperature - Old API");

        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(IntWritable.class);

        config.setMapperClass(MaxTemperatureOldAPIMapper.class);
        config.setCombinerClass(MaxTemperatureOldAPIReducer.class);
        config.setReducerClass(MaxTemperatureOldAPIReducer.class);

        config.setInputFormat(TextInputFormat.class);
        config.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(config, new Path(args[0]));
        FileOutputFormat.setOutputPath(config, new Path(args[1]));

        JobClient.runJob(config);
    }
}
