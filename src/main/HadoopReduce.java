package main;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import data.DataStructureWritable;

public class HadoopReduce extends MapReduceBase
		implements
		Reducer<DataStructureWritable, IntWritable, DataStructureWritable, IntWritable> {

	@Override
	public void reduce(DataStructureWritable key, Iterator<IntWritable> values,
			OutputCollector<DataStructureWritable, IntWritable> output,
			Reporter reporter) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		output.collect(key, new IntWritable(sum));
	}

}
