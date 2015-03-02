package biz.interretis.hadoop_tutorial.max_temperature.new_api;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class MaxTemperatureNewAPITest {

    // Systems under test
    private MaxTemperatureNewAPIMapper mapper;
    private MaxTemperatureNewAPIReducer reducer;

    // Test infrastructure
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> driver;

    // Values
    public static final LongWritable IGNORED_KEY = new LongWritable(0);

    @Before
    public void setUp() {

        mapper = new MaxTemperatureNewAPIMapper();
        reducer = new MaxTemperatureNewAPIReducer();

        driver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void single_line() throws IOException {

        // given
        driver.withInput(IGNORED_KEY, new Text("0029029070999991901010320004+64333+023450FM-12+000599999V0202301N011819999999N0000001N9-00281+99999101741ADDGF108991999999999999999999"));

        // then
        driver.withOutput(new Text("1901"), new IntWritable(-28));

        // when
        driver.runTest();
    }

    @Test
    public void whole_file() throws IOException {

        // given
        final List<String> lines = FileUtils.readLines(new File("./src/main/resources/max-temperature/text/1901"));
        for (final String line : lines) {
            driver.withInput(IGNORED_KEY, new Text(line));
        }

        // then
        driver.withOutput(new Text("1901"), new IntWritable(317));

        // when
        driver.runTest();
    }
}
