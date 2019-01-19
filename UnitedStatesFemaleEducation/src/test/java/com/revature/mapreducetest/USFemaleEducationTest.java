package com.revature.mapreducetest;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.USFemaleEducationMapper;
import com.revature.reduce.USFemaleEducationReducer;

public class USFemaleEducationTest {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	/*
	 * Setup the harnesses before every test
	 */
	@Before
	public void setup() {
		USFemaleEducationMapper mapper = new USFemaleEducationMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		USFemaleEducationReducer reducer = new USFemaleEducationReducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testUSFemaleEducationMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\",\"\","));
		
		mapDriver.withOutput(new Text("Bachelor's"), new DoubleWritable(36.00504 - 35.37453));
		mapDriver.withOutput(new Text("Bachelor's"), new DoubleWritable(37.52263 - 36.00504));
		mapDriver.withOutput(new Text("Bachelor's"), new DoubleWritable(38.44067 - 37.52263));
		mapDriver.withOutput(new Text("Bachelor's"), new DoubleWritable(39.15297 - 38.44067));
		mapDriver.withOutput(new Text("Bachelor's"), new DoubleWritable(39.89922 - 39.15297));
		mapDriver.withOutput(new Text("Bachelor's"), new DoubleWritable(40.53132 - 39.89922));
		mapDriver.withOutput(new Text("Bachelor's"), new DoubleWritable(41.12231 - 40.53132));
		mapDriver.withOutput(new Text("Bachelor's"), new DoubleWritable(20.38445 - 20.18248));
		mapDriver.withOutput(new Text("Bachelor's"), new DoubleWritable(20.68499 - 20.38445));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testUSFemaleEducationReducer() {
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		
		values.add(new DoubleWritable(0.13515));
		values.add(new DoubleWritable(0.1651894));
		values.add(new DoubleWritable(0.28922));
		values.add(new DoubleWritable(0.684984));
		
		reduceDriver.withInput(new Text("Master's"), values);
		double sum = 0;
		
		for(DoubleWritable value : values) {
			sum = sum + value.get();
		}
		
		double expectedOutput = sum / values.size();
		
		reduceDriver.withOutput(new Text("Master's"), new DoubleWritable(expectedOutput));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testUSFemaleEducationMapperReducer() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, completed Doctoral or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.DO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.125\",\"1.250\",\"1.300\",\"\","));
		
		double sum = (1.250 - 1.125) + (1.300 - 1.250);
		
		double expectedOutput = sum / 2;
		
		mapReduceDriver.withOutput(new Text("Doctoral"), new DoubleWritable(expectedOutput));
		
		mapReduceDriver.runTest();
	}
}
