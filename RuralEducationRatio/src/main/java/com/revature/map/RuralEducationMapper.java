package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RuralEducationMapper extends Mapper<LongWritable, Text, Text, Text> {
	/**
	 * The mapper's job is to take the records with the indicator code for rural population percentage or education attainment of at least Bachelor's for females
	 * and output the country name as the key and the indicator code for the record, the data point for the record between 2000 and 2015, and the year of the data point.
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();		
		String[] splitLine = line.split("\",\""); // split the line by commas between two quotation marks
		
		if(splitLine.length != 0) {
			splitLine[0] = splitLine[0].substring(1); // remove the opening quotation mark from the first field (Country Name)
			int lastFieldLength = splitLine[splitLine.length-1].length();
			splitLine[splitLine.length-1] = splitLine[splitLine.length-1].substring(0, lastFieldLength - 2); // remove the trailing quotation mark and comma from the last field
			double percentage = 0;
			int yearOfData = 0;
			// if the fourth field matches the data indicator code for education attainment of at least Bachelor's for females or rural population for females, look for data points in the split line
			if(splitLine[3].compareTo("SE.TER.CUAT.BA.FE.ZS") == 0 || splitLine[3].compareTo("SP.RUR.TOTL.FE.ZS") == 0) {
				// go backwards through the split line starting at 2015 looking for the latest data if education or all data points if rural population
				for(int field = 2015 - 1956; field >= 2000 - 1956; field--) {
					if(splitLine[field].length() > 0) {
						percentage = Double.parseDouble(splitLine[field]);
						yearOfData = field + 1956;
						context.write(new Text(splitLine[0]), new Text(splitLine[3] + "\t" + percentage + "\t" + yearOfData));
						if(splitLine[3].compareTo("SE.TER.CUAT.BA.FE.ZS") == 0) {
							break;
						}
					}
				}
			}
		}
	}
}
