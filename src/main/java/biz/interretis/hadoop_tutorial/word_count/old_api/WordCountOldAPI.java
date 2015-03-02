package biz.interretis.hadoop_tutorial.word_count.old_api;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCountOldAPI {

    public static class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);

        private final Text word = new Text();

        @Override
        public void map(final LongWritable key, final Text value, final OutputCollector<Text, IntWritable> output, final Reporter reporter) throws IOException {

            final String line = value.toString();
            final StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                final String token = tokenizer.nextToken();
                word.set(token);
                output.collect(word, ONE);
            }
        }
    }

    public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(final Text key, final Iterator<IntWritable> values, final OutputCollector<Text, IntWritable> output, final Reporter reporter) throws IOException {

            int sum = 0;
            while (values.hasNext()) {
                final IntWritable value = values.next();
                sum += value.get();
            }

            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(final String... args) throws IOException {

        final JobConf config = new JobConf(WordCountOldAPI.class);
        config.setJobName("WordCount old API");

        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(IntWritable.class);

        config.setMapperClass(WordCountMapper.class);
        config.setCombinerClass(WordCountReducer.class);
        config.setReducerClass(WordCountReducer.class);

        config.setInputFormat(TextInputFormat.class);
        config.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(config, new Path(args[0]));
        FileOutputFormat.setOutputPath(config, new Path(args[1]));

        JobClient.runJob(config);
    }
}
