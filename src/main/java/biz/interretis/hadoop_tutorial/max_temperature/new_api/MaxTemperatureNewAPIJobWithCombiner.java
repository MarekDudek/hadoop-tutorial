package biz.interretis.hadoop_tutorial.max_temperature.new_api;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureNewAPIJobWithCombiner {

    public static void main(final String... args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage MaxTemperatureNewAPIJob <input path> <output path>");
            System.exit(-1);
        }

        final Job job = new Job();

        job.setJarByClass(MaxTemperatureNewAPIJob.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureNewAPIMapper.class);
        job.setCombinerClass(MaxTemperatureNewAPIReducer.class);
        job.setReducerClass(MaxTemperatureNewAPIReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        final boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
