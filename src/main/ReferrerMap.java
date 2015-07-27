package main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import utils.DateUtils;

import data.DataStructureWritable;

public class ReferrerMap extends MapReduceBase implements
		Mapper<LongWritable, Text, DataStructureWritable, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	
	private long date;
	private String line, element;
	private int count;

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<DataStructureWritable, IntWritable> output,
			Reporter reporter) throws IOException {
		//get the line
		line = value.toString();
		//split the line
		String[] split = line.split("\t");
		//get the date
		date = DateUtils.stringToDate(split[0]);
		//get the element
		element = split[1];
		//get the value
		count = Integer.parseInt(split[2]);
		//check if the line refer to a domain
		if (!element.equals("video_downloads") && !element.equals("page_views")) {
			//increase the referrer count
			output.collect(new DataStructureWritable(date, "referrer"), one);
			//emit also the original domain statistics
			output.collect(new DataStructureWritable(date, element), new IntWritable(count));
		} else {
			//report the output
			output.collect(new DataStructureWritable(date, element), new IntWritable(count));
		}
	}

}
