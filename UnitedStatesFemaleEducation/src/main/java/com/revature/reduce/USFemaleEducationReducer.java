package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.revature.writables.PercentageYearWritable;

public class USFemaleEducationReducer extends Reducer<Text, PercentageYearWritable, Text, DoubleWritable> {
	/**
	 * The reducer's job is to sum all of the difference in percent points for a particular education level, take the average of the sum,
	 * and write out the education level as a key and the average of the sum as a value
	 */
	public void reduce(Text key, Iterable<PercentageYearWritable> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
		int counter = 0;
		for(PercentageYearWritable value : values) {
			sum = sum + value.getPercentage();
			counter++;
		}
		
		double average = sum / counter;
		
		context.write(key, new DoubleWritable(average));
	}
}
