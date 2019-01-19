package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USFemaleEducationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	/**
	 * 
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();		
		String[] splitLine = line.split("\",\""); // split the line by commas between two quotation marks
		
		if(splitLine.length != 0) {
			splitLine[0] = splitLine[0].substring(1); // remove the opening quotation mark from the first field (Country Name)
			int lastFieldLength = splitLine[splitLine.length-1].length();
			splitLine[splitLine.length-1] = splitLine[splitLine.length-1].substring(0, lastFieldLength - 2); // remove the trailing quotation mark and comma from the last field
			
			// only concerned with the United States
			if(splitLine[0].compareTo("United States") == 0) {
				String educationLevel = "";
				// look at the indicator code for bachelor's percentage, master's percentage, and doctoral percentage
				if(splitLine[3].compareTo("SE.TER.HIAT.BA.FE.ZS") == 0) {
					educationLevel = "Bachelor's";
				}
				else if(splitLine[3].compareTo("SE.TER.HIAT.MS.FE.ZS") == 0) {
					educationLevel = "Master's";
				}
				else if(splitLine[3].compareTo("SE.TER.HIAT.DO.FE.ZS") == 0) {
					educationLevel = "Doctoral";
				}
				
				// only search the rows that correspond to a non-empty education level
				if(!educationLevel.isEmpty()) {
					if((2000 - 1956) < splitLine.length) {
						double currentFieldPercentage;
						double nextFieldPercentage;
						for(int currentField = 2000 - 1956, nextField; currentField < splitLine.length; currentField = nextField) {
							nextField = currentField + 1;
							
							// ignore empty fields and the 2012 field
							if(splitLine[currentField].length() == 0 || currentField == 2012 - 1956) {
								continue;
							}
							
							currentFieldPercentage = Double.parseDouble(splitLine[currentField]);
							
							while(nextField < splitLine.length && splitLine[nextField].length() == 0) {
								nextField++;
							}
							
							if(nextField >= splitLine.length) {
								break;
							}
							
							nextFieldPercentage = Double.parseDouble(splitLine[nextField]);
							
							context.write(new Text(educationLevel), new DoubleWritable(nextFieldPercentage - currentFieldPercentage));
						}
					}
				}
			}
		}
	}
}
