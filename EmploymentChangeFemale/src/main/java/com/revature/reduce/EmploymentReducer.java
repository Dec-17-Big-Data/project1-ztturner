package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.revature.writables.OldCurrentPercentagesWritable;

public class EmploymentReducer extends Reducer<Text, OldCurrentPercentagesWritable, Text, Text> {
	/**
	 * The reducer's job is to take in the country name, the year 2000 percentage of female employment, and the latest year percentage of female employment,
	 * calculate the actual difference in percent points and relative difference in percentage for the latest year and year 2000,
	 * and output the country name, the actual difference in percent points, and the relative difference in percentage.
	 */
	public void reduce(Text key, Iterable<OldCurrentPercentagesWritable> values, Context context) throws IOException, InterruptedException {
		
		for(OldCurrentPercentagesWritable value : values) {
			// pull the 2000 data point and latest data point from the input percentage object
			double yearTwoThousandPercentage = value.getOldPercentage();
			double latestYearPercentage = value.getCurrentPercentage();
			
			double changeInPercentagePoints = latestYearPercentage - yearTwoThousandPercentage; // the absolute change in percentage points from the latest year data point and the year 2000 data point
			double relativeChange = (changeInPercentagePoints / yearTwoThousandPercentage) * 100.0; // the relative percent change between the latest year data point and the year 2000 data point
			
			context.write(key, new Text(changeInPercentagePoints + "\t" + relativeChange)); // write the country name, the absolute change in percentage points, and the relative percent change
		}				
	}
}
