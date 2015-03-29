package biz.interretis.hadoop_tutorial.filesystem;

import biz.interretis.hadoop_tutorial.utils.TextIOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URL;

public class FilesystemManipulation {

    private static final String HDFS_FILE = "hdfs://localhost/user/marek/filesystem/input/counties.csv";

    public static class FileSystemMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            super.setup(context);

            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
            TextIOUtils.printLines(HDFS_FILE);
            TextIOUtils.displayFromFilesystem(HDFS_FILE);
        }
    }

    public static void main(final String... args) throws IOException, ClassNotFoundException, InterruptedException {

        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        TextIOUtils.printLines(HDFS_FILE);
        TextIOUtils.displayFromFilesystem(HDFS_FILE);

        final Configuration config = new Configuration();
        final Job job = Job.getInstance(config, "Filesystem manipulation");

        final String input = args[0];
        final String output = args[1];


        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(FilesystemManipulation.class);

        job.setMapperClass(FileSystemMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
