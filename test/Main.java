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

import com.Bz2Parser.CleanUnlabeledMapper;

import java.lang.*;

//Driver program
public class Main {
	private static String inputPath, outputPath;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }

        inputPath = otherArgs[0];
        outputPath = otherArgs[1];
        System.out.println(outputPath);
        cleanUnlabeled();
        classifier();
        
	}

	//Cleans the unlabeled dataset
	public static void cleanUnlabeled() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "word count");
		job.setJarByClass(Main.class);
		job.setMapperClass(CleanUnlabeledMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(inputPath+"/unlabeled.csv.bz2"));
		FileOutputFormat.setOutputPath(job,new Path(outputPath+"/unlabeledCSV"));
		job.waitForCompletion(true);
	}


    
	//Helper to load all the models to cache
	private static Configuration loadModelsDistributedCache() throws Exception 
	{
        Configuration configuration = new Configuration();
        Path location = new Path(inputPath + "/models");
        FileSystem fileSystem = FileSystem.get(location.toUri(), configuration  );
        FileStatus[] partitions = fileSystem.listStatus(location);
        if (partitions != null && partitions.length > 0) {
            for (int i = 0; i < partitions.length; i++) {
				String filename = partitions[i].getPath().getName();
				if (filename.contains("SUCCESS") || filename.contains("part-r-")) continue;
                System.out.println(partitions[i].getPath().getName());
                DistributedCache.addCacheFile(partitions[i].getPath().toUri(), configuration);
            }
        }
        return configuration;
    }

	//Predicts the presence/absence of the bird
	private static void classifier() throws Exception 
	{
	    Configuration configuration = loadModelsDistributedCache();
	    configuration.set("features", "LATITUDE,LONGITUDE,YEAR,MONTH,DAY,TIME,NUMBER_OBSERVERS,POP00_SQMI,HOUSING_DENSITY,HOUSING_PERCENT_VACANT,ELEV_GT,ELEV_NED,BCR,CAUS_TEMP_AVG,CAUS_TEMP_MIN,CAUS_TEMP_MAX,CAUS_PREC,Agelaius_phoeniceus");
	    Job job = new Job(configuration, "classification");
	    job.setJarByClass(Main.class);
	    job.setMapperClass(com.Test.WekaTesterMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setNumReduceTasks(0);
	    FileInputFormat.addInputPath(job, new Path(outputPath + "/unlabeledCSV"));
	    FileOutputFormat.setOutputPath(job,
	            new Path(outputPath + "/results"));
	    boolean ok = job.waitForCompletion(true);
	    if (!ok) {
	        throw new Exception("Job failed");
	    }
}


	
}
