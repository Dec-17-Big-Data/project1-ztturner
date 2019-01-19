package com.revature.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.MaleEmploymentMapper;
import com.revature.reduce.MaleEmploymentReducer;
import com.revature.writables.OldCurrentPercentagesWritable;

public class MaleEmploymentChangeByCountry {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("usage: hadoop jar EmploymentChangeMale.jar com.revature.job.MaleEmploymentChangeByCountry inputDir outputDir");
			return;
		}
		
		Job job = Job.getInstance();		
		job.setJobName("Male Employment Change");
		
		// set the driver, mapper, and reducer classes
		job.setJarByClass(MaleEmploymentChangeByCountry.class);
		job.setMapperClass(MaleEmploymentMapper.class);
		job.setReducerClass(MaleEmploymentReducer.class);
		
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
