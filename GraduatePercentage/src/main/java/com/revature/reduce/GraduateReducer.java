package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.revature.writables.PercentageYearWritable;

public class GraduateReducer extends Reducer<Text, PercentageYearWritable, Text, Text> {
	/**
	 * The reducer's job is to take in the country name, female graduate percentage, and year
	 * and output the countries which have a female graduate percentage that is less than 30%.
	 */
	public void reduce(Text key, Iterable<PercentageYearWritable> values, Context context) throws IOException, InterruptedException {		
		for(PercentageYearWritable value : values) {
			if(value.getPercentage() < 30.0) {
				context.write(key, new Text(value.getPercentage() + "\t" + value.getYearOfData()));
			}
		}
	}
}
