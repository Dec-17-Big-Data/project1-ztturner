package com.revature.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.RuralEducationMapper;
import com.revature.reduce.RuralEducationReducer;

public class RuralEducationDriver {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("usage: hadoop jar RuralEducationRatio.jar com.revature.job.RuralEducationDriver inputDir outputDir");
			return;
		}
		
		Job job = Job.getInstance();
		job.setJobName("Education Attainment Percentage by Rural Population Percentage");
		
		// set the driver, mapper, and reducer
		job.setJarByClass(RuralEducationDriver.class);
		job.setMapperClass(RuralEducationMapper.class);
		job.setReducerClass(RuralEducationReducer.class);
		
		// set the outputs
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		// set the input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// wait for the job to complete
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}
