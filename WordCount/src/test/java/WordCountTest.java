import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cloudwick.hadoop.mapreduce.wordcount.SumReducer;
import com.cloudwick.hadoop.mapreduce.wordcount.WordMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for demonstrating MRUnit
 * Run as a JUnit test case
 * @author skangayam
 *
 */

public class WordCountTest {
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		WordMapper mapper = new WordMapper();
		SumReducer reducer = new SumReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		System.out.println("Its BEFORE Test");
	}

	/**
	 * Test case for verifying the validity of the mapper.
	 * Modify the input and output values to see the success and failure messages
	 * @throws IOException
	 */
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("a,b,c"));
		mapDriver.withOutput(new Text("a"), new IntWritable(1));
		mapDriver.addOutput(new Text("b"), new IntWritable(1));
		mapDriver.addOutput(new Text("c"), new IntWritable(1));
		mapDriver.runTest();
		System.out.println("Its at Test");
	}
	
	/**
	 * Test case for verifying the validity of the reducer
	 * Modify the input and output values to see the success and failure messages
	 * @throws IOException
	 */
	  @Test
	  public void testReducer() throws IOException {
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    //values.add(new IntWritable(1));
	    reduceDriver.withInput(new Text("a"), values);
	    reduceDriver.withOutput(new Text("a"), new IntWritable(2));
	    reduceDriver.runTest();
	  }


	

}
