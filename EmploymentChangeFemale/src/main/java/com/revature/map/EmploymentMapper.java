package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.revature.writables.OldCurrentPercentagesWritable;

public class EmploymentMapper extends Mapper<LongWritable, Text, Text, OldCurrentPercentagesWritable> {
	/**
	 * The mapper's job is to take records that contain the data indicator code for the modeled ILO estimate of employment-to-population ratio for females 15 and above
	 * and output the country name, the percentage employed in the year 2000, and the percentage employed in the latest year with data.
	 * The reason the modeled ILO estimate is used as opposed to the national estimate is that more countries have data in the modeled ILO estimate row than the national estimate row.
	 * It is assumed that the question asks for the difference in percent points between the year 2000 and the latest year with data.
	 * It is assumed that countries that do not have a year 2000 data point or any additional data points after 2000 should not be output.
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();		
		String[] splitLine = line.split("\",\""); // split the line by commas between two quotation marks
		
		if(splitLine.length != 0) {
			splitLine[0] = splitLine[0].substring(1); // remove the opening quotation mark from the first field (Country Name)
			int lastFieldLength = splitLine[splitLine.length-1].length();
			splitLine[splitLine.length-1] = splitLine[splitLine.length-1].substring(0, lastFieldLength - 2); // remove the trailing quotation mark and comma from the last field
			
			// if the fourth field matches the indicator code for employment-to-population ratio for females (modeled ILO estimate), look for data points in the row
			if(splitLine[3].toUpperCase().compareTo("SL.EMP.TOTL.SP.FE.ZS") == 0) {
				
				// check that the split line is long enough to hold the 2000 data point
				if((2000 - 1956) < splitLine.length) {
					
					// check if the 2000 data point is non-empty
					if(splitLine[2000 - 1956].length() != 0) {
						double yearTwoThousandPercentage = Double.parseDouble(splitLine[2000 - 1956]); // parse a double from the 2000 data point
						
						// go backwards through the spilt line for the latest data point in the line
						for(int field = splitLine.length - 1; field > (2000 - 1956); field--) {
							// check if the data point is non-empty
							if(splitLine[field].length() != 0) {
								double latestYearPercentage = Double.parseDouble(splitLine[field]); // parse a double from the latest data point								
								context.write(new Text(splitLine[0]), new OldCurrentPercentagesWritable(yearTwoThousandPercentage, latestYearPercentage)); // write the country name, 2000 data point, and latest data point
								break; // break out of the loop as a data point has been found
							}
						}
					}
				}
			}
		}
	}
}
