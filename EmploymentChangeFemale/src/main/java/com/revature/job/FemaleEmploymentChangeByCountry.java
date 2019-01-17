package com.revature.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.EmploymentMapper;
import com.revature.reduce.EmploymentReducer;
import com.revature.writables.OldCurrentPercentagesWritable;

public class FemaleEmploymentChangeByCountry {
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("usage: hadoop jar EmploymentChangeFemale.jar com.revature.job.FemaleEmploymentChangeByCountry inputDir outputDir");
			return;
		}
		
		Job job = Job.getInstance();		
		job.setJobName("Female Employment Change");
		
		// set the driver, mapper, and reducer classes
		job.setJarByClass(FemaleEmploymentChangeByCountry.class);
		job.setMapperClass(EmploymentMapper.class);
		job.setReducerClass(EmploymentReducer.class);
		
		// set the outputs
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OldCurrentPercentagesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the file paths for input and output
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// wait for the job to complete
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
