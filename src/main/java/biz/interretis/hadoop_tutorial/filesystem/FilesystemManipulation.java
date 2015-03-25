package biz.interretis.hadoop_tutorial.filesystem;

import org.apache.commons.io.IOUtils;
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
import java.io.InputStream;
import java.net.URL;
import java.util.List;

public class FilesystemManipulation {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static class FileSystemMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            super.setup(context);

            printLines();
        }
    }

    public static void main(final String... args) throws IOException, ClassNotFoundException, InterruptedException {

        printLines();

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

    private static void printLines() throws IOException {

        final URL fileUrl = new URL("hdfs://localhost/user/marek/filesystem/input/counties.csv");

        final InputStream stream = fileUrl.openStream();
        final List<String> lines = IOUtils.readLines(stream);
        for(final String line: lines) {
            System.out.println(line);
        }
        IOUtils.closeQuietly(stream);
    }
}
