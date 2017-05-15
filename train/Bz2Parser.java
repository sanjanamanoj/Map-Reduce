package com;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;



public class Bz2Parser {
	
	public static class CleanLabeledMapper 
	extends Mapper<Object, Text, Text, NullWritable>{
		
		//The required fields are taken from the labeled dataset 
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException
		{
				if(line.toString().contains("LATITUDE"))
					return;
				String[] fields = line.toString().split(",");
				String res="";
				res+=fields[2]+",";
				res+=fields[3]+",";
				res+=fields[4]+",";
				res+=fields[5]+",";
				res+=fields[6]+",";
				res+=fields[7]+",";
				res+=fields[16]+",";
				res+=fields[955]+",";
				res+=fields[956]+",";
				res+=fields[957]+",";
				res+=fields[958]+",";
				res+=fields[959]+",";
				res+=fields[960]+",";
				res+=fields[963]+",";
				res+=fields[964]+",";
				res+=fields[965]+",";
				res+=fields[966]+",";
				
				//If the bird column contains X replace it with 1( the observer marks 'X' if
				//he forgets the count of the bird)
    			if (fields[26].contains("X")) {
                    fields[26] = "1";
                }
    			//If the bird column contains X replace it with 0
    			if (fields[26].contains("?")) {
                    fields[26] = "0";
                }
    			//If the bird count is greater than 0, replace with 1
                if (Integer.parseInt(fields[26]) > 0) {
                    fields[26] = "1";
                }
                res += fields[26];
				String re = res.replace("?", "0").replace("X", "1");
    			context.write(new Text(re),NullWritable.get());	
			}	
	}
}

