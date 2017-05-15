

package com;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;








import com.Bz2Parser.CleanLabeledMapper;
import com.Train.TrainMapper;
import com.Train.TrainReducer;

import java.lang.*;


public class Main 
{
	private static String inputPath, outputPath;

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2)
        {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        inputPath = otherArgs[0];
        outputPath = otherArgs[1];
        System.out.println(outputPath);
        cleanLabeled();
        train();     
	}

	//Parses the labeled dataset to give cleaned useful data for model training
	public static void cleanLabeled() throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "word count");
		job.setJarByClass(Main.class);
		job.setMapperClass(CleanLabeledMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(inputPath +"/labeled.csv.bz2"));
		FileOutputFormat.setOutputPath(job,new Path(outputPath+"/labeledCSV"));
		job.waitForCompletion(true);
	}
	
	//Job used to train and evaluate the models
	public static void train() throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
	    conf.set("modelpath", outputPath+"/models");
		Job job = new Job(conf, "train");
		job.setJarByClass(Main.class);
		job.setMapperClass(TrainMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(TrainReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(outputPath +"/labeledCSV"));
		FileOutputFormat.setOutputPath(job,new Path(outputPath+"/models"));
		job.waitForCompletion(true);
	}	
}
