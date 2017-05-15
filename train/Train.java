package com;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.evaluation.NominalPrediction;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.functions.Logistic;
import weka.classifiers.functions.SimpleLogistic;
import weka.classifiers.lazy.IBk;
import weka.classifiers.meta.LogitBoost;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;

public class Train 
{
	//Evaluates the model with the given dataset
	public static Evaluation classify(Classifier model,
			Instances trainingSet, Instances testingSet,String path) throws Exception
	{
		Evaluation evaluation = new Evaluation(trainingSet);
		model.buildClassifier(trainingSet);
		weka.core.SerializationHelper.write(path,model);
		evaluation.evaluateModel(model, testingSet);
		return evaluation;
	}

	//Returns the accuracy of the model
	public static double calculateAccuracy(FastVector predictions) 
	{
		double correct = 0;
		for (int i = 0; i < predictions.size(); i++) 
		{
			NominalPrediction np = (NominalPrediction) predictions.elementAt(i);
			if (np.predicted() == np.actual()) 
			{
				correct++;
			}
		}
		return 100 * correct / predictions.size();
	}
 
	//Splits the dataset into training and testing splits
	public static Instances[][] crossValidationSplit(Instances data, int numberOfFolds) 
	{
		Instances[][] split = new Instances[2][numberOfFolds];
		for (int i = 0; i < numberOfFolds; i++) 
		{
			split[0][i] = data.trainCV(numberOfFolds, i);
			split[1][i] = data.testCV(numberOfFolds, i);
		}
		return split;
	}
	
	//The cleaned data from the labeled dataset is sent to different reducers to train and
	// evaluate models
	public static class TrainMapper extends Mapper<Object, Text, IntWritable, Text> 
	{
		private static int k;
        Random random;
        private static double probability;
        
        public void setup(Context context) throws IOException, InterruptedException 
        {
            k = context.getConfiguration().getInt("k-value", 10);
            random = new Random();
            probability = (double) 1/(k-1);
        }
        
		public void map(Object obj, Text line, Context context) throws IOException, InterruptedException
		{
			String lineStr = line.toString();
            if (lineStr.contains("LATITUDE")) return;
            int randnum = random.nextInt(100);
            String[] temp = lineStr.split(",");
            //Since the data is highly biased, if the record indicates presence of the 
            // bird, it is definitely included
            if(Integer.parseInt(temp[temp.length-1])>0)
            	context.write(new IntWritable(randnum%k), line);
            for (int i = 0; i< k;i++) {
                int rand = random.nextInt(10);
                if (probability == (double) 1/rand) {
                    context.write(new IntWritable(i), line);
                }
            }
           
		}
	}
	
	
	 public static class TrainReducer extends Reducer<IntWritable, Text, NullWritable, NullWritable> {
	        private static final String header = "LATITUDE,LONGITUDE,YEAR,MONTH,DAY,TIME,NUMBER_OBSERVERS,POP00_SQMI,HOUSING_DENSITY,HOUSING_PERCENT_VACANT,ELEV_GT,ELEV_NED,BCR,CAUS_TEMP_AVG,CAUS_TEMP_MIN,CAUS_TEMP_MAX,CAUS_PREC,Agelaius_phoeniceus";
	        public void reduce(IntWritable key, Iterable<Text> values, Context context)
	        {
	        	//Creating the instances
	        	String[] features = header.split(",");
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
	    		 
	    		 Instances trainset = new Instances("Rel", fvWekaAttributes, 1);
	    		 trainset.setClassIndex(features.length-1);
	    		 
	    		 ArrayList<Instance> records = new ArrayList<Instance>();
	    		 for(Text v : values)
	    		 {
	    			 String[] temp1 = v.toString().split(",");
	    			 
	    			 Instance iExample = new Instance(temp1.length);
		    		 for(int i=0;i<temp1.length;i++)
		    		 {
		    			 iExample.setValue((Attribute)fvWekaAttributes.elementAt(i), Double.parseDouble(temp1[i]));
		    		 }
		    		 records.add(iExample);
	    		 }
	  		 
	     		 for(Instance i : records)
	    		 {
	    			 trainset.add(i);
	    		 }
	     		 
	     		//Converts the last field of the created instance to nominal
	     		NumericToNominal convert= new NumericToNominal();
	            String[] options= new String[2];
	            options[0]="-R";
	            options[1]="18"; 
	            try 
	            {
		            convert.setOptions(options);
		            convert.setInputFormat(trainset);
		            Instances newData=Filter.useFilter(trainset, convert);
		    		Instances[][] split = crossValidationSplit(newData, 10);
		            Instances[] trainingSplits = split[0];
		    		Instances[] testingSplits = split[1];
		     
		    		Classifier[] models = { 
		    				new Logistic() 
		    		};
		     
		    		for (int j = 0; j < models.length; j++)
		    		{
		    			// Collect every group of predictions for current model in a FastVector
		    			FastVector predictions = new FastVector();
		     
		    			// For each training-testing split pair, train and test the classifier
		    			for (int i = 0; i < trainingSplits.length; i++) 
		    			{
		    				String path = context.getConfiguration().get("modelpath")+"/model"+key.get();
		    				Evaluation validation = classify(models[j], trainingSplits[i], testingSplits[i],path);
		    				predictions.appendElements(validation.predictions());
		     
		    			}
		     
		    			// Calculate overall accuracy of current classifier on all splits
		    			double accuracy = calculateAccuracy(predictions);
		     
		    			
		    			System.out.println("Accuracy of " + models[j].getClass().getSimpleName() + ": "
		    					+ String.format("%.2f%%", accuracy)
		    					+ "\n---------------------------------");
		    		}
	            }
	            catch(Exception e)
	            {
	            	
	            }

	        }
	 }
}
