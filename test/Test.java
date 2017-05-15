package com;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
//import weka.core.DenseInstance;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;

public class Test 
{
	public static class WekaTesterMapper extends Mapper<Object, Text, Text, NullWritable> {
		ArrayList<Classifier> models;
		private int mapperId;

		// Setup loads the trained models from the distributed cache into an array list of classifiers.
		public void setup(Context context) throws IOException, InterruptedException
		{
			//If mapper id is 0, emit the header in setup
			mapperId = context.getConfiguration().getInt("mapred.task.partition",-1);
			if (mapperId == 0) context.write(new Text("SAMPLING_EVENT_ID, SAW_AGELAIUS_PHOENICEUS"),NullWritable.get());

			models = new ArrayList<Classifier>();
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
			for (int i=0;i<cacheFiles.length;i++) {
				Classifier cls = null;
				Path getPath = new Path(cacheFiles[i].getPath());
				if(getPath.toString().contains("SUCCESS")) return;

				if(  getPath.toString().contains("part"))return;

				try {
					cls = (Classifier) weka.core.SerializationHelper.read(getPath.toString());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				models.add(cls);




			}
			System.out.println("model size:"+models.size());
		}

		
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			//Getting features from context.
			String col = context.getConfiguration().get("features");
			String[] features = col.split(",");
			String l = line.toString();	    	
			//Ignore the header from the input data	
			if (l.contains("LATITUDE")) return;

			String[] temp1 =l.split(",");
				
			//Build features for creating instances.
			FastVector fvWekaAttributes = new FastVector(features.length);
			Attribute[] att = new Attribute[features.length];
			for(int i=0;i<features.length;i++)
			{
				att[i] = new Attribute(features[i]);
			}
			for(int i=0;i<features.length;i++)
			{
				fvWekaAttributes.addElement(att[i]);
			}

			//Create an instance from the attributes built previously.
			Instances isTrainingSet = new Instances("Rel", fvWekaAttributes, 1);
			isTrainingSet.setClassIndex(isTrainingSet.numAttributes()-1);

			Instance iExample = new Instance(features.length);
			for(int i=0;i<features.length;i++)
			{
				iExample.setValue((Attribute)fvWekaAttributes.elementAt(i), Double.parseDouble(temp1[i]));
			}

			isTrainingSet.add(iExample);

			//Set the last attribute ie the bird to be predicted as a nominal of 1 or 0.
			Instances newData=null;
			NumericToNominal convert= new NumericToNominal();
			String[] options= new String[2];
			options[0]="-R";
			options[1]="18"; 
			try 
			{
				convert.setOptions(options);
				convert.setInputFormat(isTrainingSet);

				newData=Filter.useFilter(isTrainingSet, convert);
			}
			catch(Exception e)
			{

			}

			//System.out.println(iExample);
			double total=0;
			int pred=0;
			//For each mode make the prediction for a given sampling id.
			for(int i=0;i<models.size();i++)
			{
				//System.out.println(isTrainingSet.firstInstance());
				try {
					total+= models.get(i).classifyInstance(newData.firstInstance());
					if(total/models.size()>=0.5)
						pred=1;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			//Write predictions to file.
			context.write(new Text(temp1[temp1.length-1] + "," + pred), NullWritable.get());


		}
	}
}
