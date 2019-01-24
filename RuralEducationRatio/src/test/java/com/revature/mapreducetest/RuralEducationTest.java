package com.revature.mapreducetest;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.RuralEducationMapper;
import com.revature.reduce.RuralEducationReducer;

public class RuralEducationTest {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	
	/*
	 * Setup the harnesses before every test
	 */
	@Before
	public void setup() {
		/*
		 * Setup the mapper harness
		 */
		RuralEducationMapper mapper = new RuralEducationMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		/*
		 * Setup the reducer harness
		 */
		RuralEducationReducer reducer = new RuralEducationReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);
		
		/*
		 * Setup the mapper-reducer harness
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	/*
	 * Test the mapper with an education record
	 */
	@Test
	public void testRuralEducationMapperEducationRecord() {
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"31.39076\",\"32.00147\",\"32.67396\",\"\","));
		
		mapDriver.withOutput(new Text("United States"), new Text("SE.TER.CUAT.BA.FE.ZS\t32.67396\t2015"));
		
		mapDriver.runTest();
	}
	
	/*
	 * Test the mapper with a rural population record
	 */
	@Test
	public void testRuralEducationMapperRuralPopulationRecord() {
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Rural population, female (% of total)\",\"SP.RUR.TOTL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"13.1234359794244\",\"\",\"\",\"\",\"\",\"12.7427957167073\",\"\",\"\",\"\",\"\",\"12.3234331472219\",\"\",\"\",\"\",\"\",\"11.3129059577151\",\"\",\"\",\"\",\"\",\"10.3863384809993\",\"\",\"\",\"\",\"\",\"9.93674664448663\",\"\",\"\",\"\",\"\",\"9.50372462737713\",\"\",\"\",\"\",\"\",\"9.06445788876347\",\"\","));
		
		mapDriver.withOutput(new Text("United States"), new Text("SP.RUR.TOTL.FE.ZS\t9.06445788876347\t2015"));
		mapDriver.withOutput(new Text("United States"), new Text("SP.RUR.TOTL.FE.ZS\t9.50372462737713\t2010"));
		mapDriver.withOutput(new Text("United States"), new Text("SP.RUR.TOTL.FE.ZS\t9.93674664448663\t2005"));
		mapDriver.withOutput(new Text("United States"), new Text("SP.RUR.TOTL.FE.ZS\t10.3863384809993\t2000"));

		
		mapDriver.runTest();
	}
	
	/*
	 * Test the reducer
	 */
	@Test
	public void testRuralEducationReducer() {
		List<Text> values = new ArrayList<Text>();
		
		values.add(new Text("SP.RUR.TOTL.FE.ZS\t9.06445788876347\t2015"));
		values.add(new Text("SP.RUR.TOTL.FE.ZS\t9.50372462737713\t2010"));
		values.add(new Text("SP.RUR.TOTL.FE.ZS\t9.93674664448663\t2005"));
		values.add(new Text("SP.RUR.TOTL.FE.ZS\t10.3863384809993\t2000"));
		values.add(new Text("SE.TER.CUAT.BA.FE.ZS\t32.67396\t2015"));
		
		reduceDriver.withInput(new Text("United States"), values);
		
		double expectedOutput = 32.67396 / 9.06445788876347;
		
		reduceDriver.withOutput(new Text("United States"), new DoubleWritable(expectedOutput));
		
		reduceDriver.runTest();
	}
	
	/*
	 * Test the mapper-reducer combination
	 */
	@Test
	public void testRuralEducationMapperReducer() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"31.39076\",\"32.00147\",\"32.67396\",\"\","));
		mapReduceDriver.withInput(new LongWritable(2), new Text("\"United States\",\"USA\",\"Rural population, female (% of total)\",\"SP.RUR.TOTL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"13.1234359794244\",\"\",\"\",\"\",\"\",\"12.7427957167073\",\"\",\"\",\"\",\"\",\"12.3234331472219\",\"\",\"\",\"\",\"\",\"11.3129059577151\",\"\",\"\",\"\",\"\",\"10.3863384809993\",\"\",\"\",\"\",\"\",\"9.93674664448663\",\"\",\"\",\"\",\"\",\"9.50372462737713\",\"\",\"\",\"\",\"\",\"9.06445788876347\",\"\","));

		double expectedOutput = 32.67396 / 9.06445788876347;
		
		mapReduceDriver.withOutput(new Text("United States"), new DoubleWritable(expectedOutput));
		
		mapReduceDriver.runTest();
	}
}
