package com.revature.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.revature.map.GraduateMapper;
import com.revature.reduce.GraduateReducer;
import com.revature.writables.PercentageYearWritable;

public class GraduatePercentageByCountry {
	public static void main(String[] args) throws Exception {
		try {
			
			if(args.length != 2) {
				System.out.println("usage: hadoop jar GraduatePercentage.jar com.revature.job.GraduatePercentageByCountry inputDir outputDir");
				return;
			}
			
			Job job = Job.getInstance();
			job.setJobName("Graduate Percentage By Country");
			// set the driver, mapper, and reducer classes
			job.setJarByClass(GraduatePercentageByCountry.class);
			job.setMapperClass(GraduateMapper.class);
			job.setReducerClass(GraduateReducer.class);
			// set the output key:value pair types
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(PercentageYearWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			// set the path for the input and output
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			// wait for the job to complete
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
