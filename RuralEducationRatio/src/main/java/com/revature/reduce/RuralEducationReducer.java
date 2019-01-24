package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RuralEducationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	/**
	 * The reducer's job is to take and parse the text input and output the country name and the ratio of the education attainment and the rural population.
	 * An assumption was made that if the latest data point for education does not have a corresponding rural population data point, part of the difference for rural population percentage
	 * between the data point year previous to the education data point year and the data point year after the education data point year will be added or subtracted
	 * from the data point previous to the education data point year.
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double educationAttainmentPercentage = 0;
		int educationYearOfData = 0;
		double ruralPercentageDataPoint = 0;
		int ruralYearDataPoint = 0;
		List<Double> ruralPopulationPercentages = new ArrayList<Double>();
		List<Integer> ruralYearsOfData = new ArrayList<Integer>();
		
		for(Text value : values) {
			String[] splitLine = value.toString().split("\t");
			
			// if the first field has the indicator code for education attainment, parse the second and third fields for the education variables
			if(splitLine[0].compareTo("SE.TER.CUAT.BA.FE.ZS") == 0) {
				educationAttainmentPercentage = Double.parseDouble(splitLine[1]);
				educationYearOfData = Integer.parseInt(splitLine[2]);
			}
			// if the first field has the indicator code for rural population, parse the second and third fields for the rural population variables and add the variables to the list
			else if(splitLine[0].compareTo("SP.RUR.TOTL.FE.ZS") == 0) {
				ruralPercentageDataPoint = Double.parseDouble(splitLine[1]);
				ruralYearDataPoint = Integer.parseInt(splitLine[2]);
				for(int index = 0; index <= ruralYearsOfData.size(); index++) {
					// if the year the data comes is less than the year in the current index, insert the data point into that index
					if(index == ruralYearsOfData.size() || ruralYearDataPoint < ruralYearsOfData.get(index)) {
						ruralYearsOfData.add(index, ruralYearDataPoint);
						ruralPopulationPercentages.add(index, ruralPercentageDataPoint);
						break;
					}
				}
			}
		}
		
		ruralPercentageDataPoint = 0;
		ruralYearDataPoint = 0;
		
		if(educationAttainmentPercentage > 0 && educationYearOfData > 0) {
			// look through the years of the data points for rural population for the appropriate rural population percentage
			for(int index = 0; index < ruralYearsOfData.size(); index++) {
				// if the education year is equal to the rural population year, use that rural population percentage
				if(educationYearOfData == ruralYearsOfData.get(index)) {
					ruralPercentageDataPoint = ruralPopulationPercentages.get(index);
					ruralYearDataPoint = ruralYearsOfData.get(index);
					break;
				}
				
				if(index < ruralYearsOfData.size() - 1) {
					// if the education year is between the current rural population year and the next rural population year
					if(educationYearOfData > ruralYearsOfData.get(index) && educationYearOfData < ruralYearsOfData.get(index + 1)) {
						// find the difference between the current rural population year percentage and the next rural population year percentage and divide by the number of years between the next year and current year
						double differenceIntervalRural = (ruralPopulationPercentages.get(index + 1) - ruralPopulationPercentages.get(index)) / (ruralYearsOfData.get(index + 1) - ruralYearsOfData.get(index));
						int differenceInYears = educationYearOfData - ruralYearsOfData.get(index); // subtract the year of the education and the current rural population year
						ruralPercentageDataPoint = ruralPopulationPercentages.get(index) + (differenceIntervalRural * differenceInYears); // add the interval times the difference between education year and current rural population year
						ruralYearDataPoint = educationYearOfData;
						break;
					}
				}
			}
		}
		
		if(ruralPercentageDataPoint > 0 && ruralYearDataPoint > 0) {
			context.write(key, new DoubleWritable(educationAttainmentPercentage / ruralPercentageDataPoint));
		}
	}
}
