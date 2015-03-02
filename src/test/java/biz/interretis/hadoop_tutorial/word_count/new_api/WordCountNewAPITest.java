package biz.interretis.hadoop_tutorial.word_count.new_api;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.google.common.collect.Lists.newArrayList;

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

    public static final IntWritable COUNT_ONE = new IntWritable(1);

    public static final Text ONE = new Text("one");
    public static final Text TWO = new Text("two");
    public static final Text THREE = new Text("three");

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

        // then
        mapDriver.withOutput(ONE, COUNT_ONE);

        mapDriver.withOutput(ONE, COUNT_ONE);
        mapDriver.withOutput(TWO, COUNT_ONE);

        mapDriver.withOutput(ONE, COUNT_ONE);
        mapDriver.withOutput(TWO, COUNT_ONE);
        mapDriver.withOutput(THREE, COUNT_ONE);

        // when
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {

        // given
        reduceDriver.withInput(ONE, newArrayList(COUNT_ONE, COUNT_ONE, COUNT_ONE));
        reduceDriver.withInput(TWO, newArrayList(COUNT_ONE, COUNT_ONE));
        reduceDriver.withInput(THREE, newArrayList(COUNT_ONE));


        // then
        reduceDriver.withOutput(ONE, new IntWritable(3));
        reduceDriver.withOutput(TWO, new IntWritable(2));
        reduceDriver.withOutput(THREE, new IntWritable(1));

        // when
        reduceDriver.runTest();
    }

    @Test
    public void testJob() throws IOException {

        // given
        mapReduceDriver.withInput(IGNORED_KEY, new Text("one one two one two three"));

        // then
        mapReduceDriver.withOutput(ONE, new IntWritable(3));
        mapReduceDriver.withOutput(THREE, new IntWritable(1));
        mapReduceDriver.withOutput(TWO, new IntWritable(2));

        // when
        mapReduceDriver.runTest();
    }
}
