package com.revature.testmapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.GraduateMapper;
import com.revature.reduce.GraduateReducer;
import com.revature.writables.PercentageYearWritable;

public class GraduatePercentageTest {
	
	// Harnesses to test the mapper, reducer, and the mapper-reducer combination
	private MapDriver<LongWritable, Text, Text, PercentageYearWritable> mapDriver;
	private ReduceDriver<Text, PercentageYearWritable, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, PercentageYearWritable, Text, Text> mapReduceDriver;
	
	/*
	 * Setup the harnesses before every test
	 */
	@Before
	public void setup() {
		/*
		 * Setup the mapper harness
		 */
		GraduateMapper mapper = new GraduateMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, PercentageYearWritable>();
		mapDriver.setMapper(mapper);
		
		/*
		 * Setup the reducer harness
		 */
		GraduateReducer reducer = new GraduateReducer();
		reduceDriver = new ReduceDriver<Text, PercentageYearWritable, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		/*
		 * Setup the mapper-reducer harness
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, PercentageYearWritable, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	/*
	 * Test the mapper.
	 */
	@Test
	public void testGraduatePercentageMapper() {
		mapDriver.withInput(new LongWritable(2), new Text("\"Canada\",\"CAN\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"37.13032\",\"37.66244\",\"\",\"\",\"40.02637\",\"\",\"42.55447\",\"49.70556\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));

		mapDriver.withOutput(new Text("Canada"), new PercentageYearWritable(49.70556, 2005));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testGraduatePercentageReducer() {
		List<PercentageYearWritable> value = new ArrayList<PercentageYearWritable>();
		
		value.add(new PercentageYearWritable(0.99827, 2013));
		
		reduceDriver.withInput(new Text("Zimbabwe"), value);

		reduceDriver.withOutput(new Text("Zimbabwe"), new Text(0.99827 + " " + 2013));
		
		reduceDriver.runTest();
	}
	
	/*
	 * Test the mapper-reducer combination
	 */
	@Test
	public void testGraduatePercentageMapperReducer() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Country Name\",\"Country Code\",\"Indicator Name\",\"Indicator Code\",\"1960\",\"1961\",\"1962\",\"1963\",\"1964\",\"1965\",\"1966\",\"1967\",\"1968\",\"1969\",\"1970\",\"1971\",\"1972\",\"1973\",\"1974\",\"1975\",\"1976\",\"1977\",\"1978\",\"1979\",\"1980\",\"1981\",\"1982\",\"1983\",\"1984\",\"1985\",\"1986\",\"1987\",\"1988\",\"1989\",\"1990\",\"1991\",\"1992\",\"1993\",\"1994\",\"1995\",\"1996\",\"1997\",\"1998\",\"1999\",\"2000\",\"2001\",\"2002\",\"2003\",\"2004\",\"2005\",\"2006\",\"2007\",\"2008\",\"2009\",\"2010\",\"2011\",\"2012\",\"2013\",\"2014\",\"2015\",\"2016\","));
		mapReduceDriver.withInput(new LongWritable(2), new Text("\"Canada\",\"CAN\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"37.13032\",\"37.66244\",\"\",\"\",\"40.02637\",\"\",\"42.55447\",\"49.70556\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		mapReduceDriver.withInput(new LongWritable(3), new Text("\"Canada\",\"CAN\",\"Gross graduation ratio, tertiary, male (%)\",\"SE.TER.CMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"24.77673\",\"24.74056\",\"\",\"\",\"24.93079\",\"\",\"25.55834\",\"29.7915\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		mapReduceDriver.withInput(new LongWritable(4), new Text("\"Congo, Dem. Rep.\",\"COD\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		mapReduceDriver.withInput(new LongWritable(5), new Text("\"Congo, Dem. Rep.\",\"COD\",\"Gross graduation ratio, tertiary, male (%)\",\"SE.TER.CMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		mapReduceDriver.withInput(new LongWritable(6), new Text("\"Zimbabwe\",\"ZWE\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.99827\",\"\",\"\",\"\","));
		mapReduceDriver.withInput(new LongWritable(7), new Text("\"Zimbabwe\",\"ZWE\",\"Gross graduation ratio, tertiary, male (%)\",\"SE.TER.CMPL.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.14281\",\"\",\"\",\"\","));

		mapReduceDriver.addOutput(new Text("Zimbabwe"), new Text(0.99827 + " " + 2013));
		
		mapReduceDriver.runTest();
	}
}
