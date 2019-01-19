package com.revature.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.USFemaleEducationMapper;
import com.revature.reduce.USFemaleEducationReducer;

public class USFemaleEducationDriver {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("usage: hadoop jar UnitedStatesFemaleEducation.jar com.revature.job.USFemaleEducationDriver inputDir outputDir");
			return;
		}
		
		Job job = Job.getInstance();
		job.setJobName("United States Change In Female Education");
		
		// set the driver, mapper, and reducer
		job.setJarByClass(USFemaleEducationDriver.class);
		job.setMapperClass(USFemaleEducationMapper.class);
		job.setReducerClass(USFemaleEducationReducer.class);
		
		// set the outputs
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		// set the input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// wait for the job to complete
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
