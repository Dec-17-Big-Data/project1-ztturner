package com.revature.testmapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FemaleEmploymentMapper;
import com.revature.reduce.FemaleEmploymentReducer;
import com.revature.writables.OldCurrentPercentagesWritable;

public class FemaleEmploymentPercentageTest {
	private MapDriver<LongWritable, Text, Text, OldCurrentPercentagesWritable> mapDriver;
	private ReduceDriver<Text, OldCurrentPercentagesWritable, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, OldCurrentPercentagesWritable, Text, Text> mapReduceDriver;
	
	/*
	 * Setup the harnesses before every test
	 */
	@Before
	public void setup() {
		/*
		 * Setup the map harness
		 */
		FemaleEmploymentMapper mapper = new FemaleEmploymentMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, OldCurrentPercentagesWritable>();
		mapDriver.setMapper(mapper);
		
		/*
		 * Setup the reduce harness
		 */
		FemaleEmploymentReducer reducer = new FemaleEmploymentReducer();
		reduceDriver = new ReduceDriver<Text, OldCurrentPercentagesWritable, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		/*
		 * Setup the map-reduce combination harness
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, OldCurrentPercentagesWritable, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testFemaleEmploymentMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"52.5610008239746\",\"52.5690002441406\",\"52.9469985961914\",\"54.1669998168945\",\"54.6139984130859\",\"55.0340003967285\",\"55.7900009155273\",\"56.1259994506836\",\"56.4620018005371\",\"56.6199989318848\",\"56.1189994812012\",\"55.2980003356934\",\"55.1599998474121\",\"55.0439987182617\",\"55.3009986877441\",\"55.7270011901855\",\"55.7519989013672\",\"55.3969993591309\",\"53.5410003662109\",\"52.6469993591309\",\"52.1679992675781\",\"52.2569999694824\",\"52.3470001220703\",\"52.693000793457\",\"53.1230010986328\",\"53.2099990844727\","));
	
		mapDriver.withOutput(new Text("United States"), new OldCurrentPercentagesWritable(56.6199989318848, 53.2099990844727));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testFemaleEmploymentReducer() {
		List<OldCurrentPercentagesWritable> value = new ArrayList<OldCurrentPercentagesWritable>();
		value.add(new OldCurrentPercentagesWritable(20.0, 25.0));
		
		reduceDriver.withInput(new Text("Canada"), value);
		
		double expectedAbsolute = 25.0 - 20.0;
		double expectedRelative = (expectedAbsolute / 20.0) * 100;
		
		reduceDriver.withOutput(new Text("Canada"), new Text(expectedAbsolute + "\t" + expectedRelative));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testFemaleEmploymentMapperReducer() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"52.5610008239746\",\"52.5690002441406\",\"52.9469985961914\",\"54.1669998168945\",\"54.6139984130859\",\"55.0340003967285\",\"55.7900009155273\",\"56.1259994506836\",\"56.4620018005371\",\"57.125\",\"56.1189994812012\",\"55.2980003356934\",\"55.1599998474121\",\"55.0439987182617\",\"55.3009986877441\",\"55.7270011901855\",\"55.7519989013672\",\"55.3969993591309\",\"53.5410003662109\",\"52.6469993591309\",\"52.1679992675781\",\"52.2569999694824\",\"52.3470001220703\",\"52.693000793457\",\"53.1230010986328\",\"60.0000\","));
		
		double expectedAbsolute = 60.0 - 57.125;
		double expectedRelative = (expectedAbsolute / 57.125) * 100;
		
		mapReduceDriver.withOutput(new Text("United States"), new Text(expectedAbsolute + "\t" + expectedRelative));
		
		mapReduceDriver.runTest();
	}
}
