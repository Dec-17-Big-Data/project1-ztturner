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
import com.revature.writables.PercentageYearWritable;

public class USFemaleEducationTest {
	private MapDriver<LongWritable, Text, Text, PercentageYearWritable> mapDriver;
	private ReduceDriver<Text, PercentageYearWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, PercentageYearWritable, Text, DoubleWritable> mapReduceDriver;
	
	/*
	 * Setup the harnesses before every test
	 */
	@Before
	public void setup() {
		USFemaleEducationMapper mapper = new USFemaleEducationMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, PercentageYearWritable>();
		mapDriver.setMapper(mapper);
		
		USFemaleEducationReducer reducer = new USFemaleEducationReducer();
		reduceDriver = new ReduceDriver<Text, PercentageYearWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, PercentageYearWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testUSFemaleEducationMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\",\"\","));
		
		mapDriver.withOutput(new Text("Bachelor's"), new PercentageYearWritable((36.00504 - 35.37453), 2005));
		mapDriver.withOutput(new Text("Bachelor's"), new PercentageYearWritable((37.52263 - 36.00504), 2006));
		mapDriver.withOutput(new Text("Bachelor's"), new PercentageYearWritable((38.44067 - 37.52263), 2008));
		mapDriver.withOutput(new Text("Bachelor's"), new PercentageYearWritable((39.15297 - 38.44067), 2009));
		mapDriver.withOutput(new Text("Bachelor's"), new PercentageYearWritable((39.89922 - 39.15297), 2010));
		mapDriver.withOutput(new Text("Bachelor's"), new PercentageYearWritable((40.53132 - 39.89922), 2011));
		mapDriver.withOutput(new Text("Bachelor's"), new PercentageYearWritable((41.12231 - 40.53132), 2012));
		mapDriver.withOutput(new Text("Bachelor's"), new PercentageYearWritable((20.38445 - 20.18248), 2014));
		mapDriver.withOutput(new Text("Bachelor's"), new PercentageYearWritable((20.68499 - 20.38445), 2015));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testUSFemaleEducationReducer() {
		List<PercentageYearWritable> values = new ArrayList<PercentageYearWritable>();
		
		values.add(new PercentageYearWritable(0.13515, 2013));
		values.add(new PercentageYearWritable(0.1651894,2014));
		values.add(new PercentageYearWritable(0.28922, 2015));
		values.add(new PercentageYearWritable(0.684984, 2016));
		
		reduceDriver.withInput(new Text("Master's"), values);
		double sum = 0;
		
		for(PercentageYearWritable value : values) {
			sum = sum + value.getPercentage();
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
