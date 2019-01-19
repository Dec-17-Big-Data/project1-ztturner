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

import com.revature.map.MaleEmploymentMapper;
import com.revature.reduce.MaleEmploymentReducer;
import com.revature.writables.OldCurrentPercentagesWritable;

public class MaleEmploymentPercentageTest {
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
		MaleEmploymentMapper mapper = new MaleEmploymentMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, OldCurrentPercentagesWritable>();
		mapDriver.setMapper(mapper);
		
		/*
		 * Setup the reduce harness
		 */
		MaleEmploymentReducer reducer = new MaleEmploymentReducer();
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
	public void testMaleEmploymentMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("\"United Kingdom\",\"GBR\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"67.0250015258789\",\"64.3119964599609\",\"62.8279991149902\",\"63.2639999389648\",\"63.931999206543\",\"63.9939994812012\",\"64.911003112793\",\"65.3730010986328\",\"65.6330032348633\",\"66.0690002441406\",\"66.036003112793\",\"65.5930023193359\",\"66.0210037231445\",\"65.9449996948242\",\"65.6949996948242\",\"65.6579971313477\",\"65.7180023193359\",\"65.5029983520508\",\"63.3919982910156\",\"62.8209991455078\",\"62.7179985046387\",\"63.1020011901855\",\"63.3040008544922\",\"64.4349975585938\",\"64.9629974365234\",\"65.1429977416992\","));
		
		mapDriver.withOutput(new Text("United Kingdom"), new OldCurrentPercentagesWritable(66.0690002441406 ,65.1429977416992));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testMaleEmploymentReducer() {
		List<OldCurrentPercentagesWritable> value = new ArrayList<OldCurrentPercentagesWritable>();
		value.add(new OldCurrentPercentagesWritable(45.235, 46.235));
		
		reduceDriver.withInput(new Text("Germany"), value);
		
		double expectedAbsolute = 46.235 - 45.235;
		double expectedRelative = (expectedAbsolute / 45.235) * 100;
		
		reduceDriver.withOutput(new Text("Germany"), new Text(expectedAbsolute + "\t" + expectedRelative));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMaleEmploymentMapperReducer() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Italy\",\"ITA\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"62.1619987487793\",\"60.2879981994629\",\"59.2700004577637\",\"57.6230010986328\",\"56.5800018310547\",\"56.2630004882813\",\"56.0050010681152\",\"55.9440002441406\",\"56.0709991455078\",\"56.1699981689453\",\"56.484001159668\",\"57.0009994506836\",\"57.382999420166\",\"57.8540000915527\",\"57.2290000915527\",\"57.4879989624023\",\"57.4770011901855\",\"57.0579986572266\",\"55.5309982299805\",\"54.6180000305176\",\"54.2169990539551\",\"53.2569999694824\",\"51.6240005493164\",\"51.3880004882813\",\"51.5859985351563\",\"51.7649993896484\","));
		
		double expectedAbsolute = 51.7649993896484 - 56.1699981689453;
		double expectedRelative = (expectedAbsolute / 56.1699981689453) * 100;
		
		mapReduceDriver.addOutput(new Text("Italy"), new Text(expectedAbsolute + "\t" + expectedRelative));
		
		mapReduceDriver.runTest();
	}
}
