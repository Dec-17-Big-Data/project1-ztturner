package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.revature.writables.PercentageYearWritable;

public class GraduateMapper extends Mapper<LongWritable, Text, Text, PercentageYearWritable> {
	/**
	 * The mapper's job is to take the records that contain the data incicator code for gross graduation ratio from tertiary education for females
	 * and output the country name, the percentage of graduates, and year of the latest data point.
	 * An assumption was made that the required graduation statistic is for tertiary education.
	 * An assumption was made that countries that did not have any data points for gross graduation ratio should not be included.
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();		
		String[] splitLine = line.split("\",\""); // split the line by commas between two quotation marks
		
		if(splitLine.length != 0) {
			splitLine[0] = splitLine[0].substring(1); // remove the opening quotation mark from the first field (Country Name)
			int lastFieldLength = splitLine[splitLine.length-1].length();
			splitLine[splitLine.length-1] = splitLine[splitLine.length-1].substring(0, lastFieldLength - 2); // remove the trailing quotation mark and comma from the last field
			
			// if the fourth field matches the required data indicator code for gross graduation ratio from tertiary education for females, look for a data point in the split line
			if(splitLine[3].compareTo("SE.TER.CMPL.FE.ZS") == 0) {
				double percentage = 0;
				int yearOfData = 0; // the year the percentage data comes from
				// go backwards through the split line to find the latest data first
				for(int field = splitLine.length - 1; field > 3; field--) {
					if(splitLine[field].length() > 0) {
						percentage = Double.parseDouble(splitLine[field]);
						yearOfData = field + 1956;
						context.write(new Text(splitLine[0]), new PercentageYearWritable(percentage, yearOfData));
						break;
					}
				}
			}
		}
	}
}
