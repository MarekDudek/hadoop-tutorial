package biz.interretis.hadoop_tutorial.word_count.new_api;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertThat;

public class WordCountNewAPITest {

    // Systems under test
    private WordCountNewAPI.WordCountMapper mapper;
    private WordCountNewAPI.WordCountReducer reducer;

    // Test infrastructure
    private MapDriver<Text, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    private MapReduceDriver<Text, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    // Values
    private static final Text IGNORED_KEY = new Text("ignored");

    @Before
    public void setUp() {

        // given
        mapper = new WordCountNewAPI.WordCountMapper();
        reducer = new WordCountNewAPI.WordCountReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {

        // given
        mapDriver.withInput(IGNORED_KEY, new Text("one one two one two three"));

        mapDriver.withOutput(new Text("one"), new IntWritable(1));
        mapDriver.withOutput(new Text("one"), new IntWritable(1));
        mapDriver.withOutput(new Text("two"), new IntWritable(1));
        mapDriver.withOutput(new Text("one"), new IntWritable(1));
        mapDriver.withOutput(new Text("two"), new IntWritable(1));
        mapDriver.withOutput(new Text("three"), new IntWritable(1));

        // when
        mapDriver.runTest();
    }
}
