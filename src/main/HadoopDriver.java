package main;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import data.DataStructureWritable;

public class HadoopDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf, HadoopDriver.class);

		Path in = new Path(arg0[0]);
		Path out = new Path(arg0[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJobName("HadoopProject");

		job.setMapperClass(HadoopMap.class);
		job.setCombinerClass(HadoopReduce.class);
		job.setReducerClass(HadoopReduce.class);

		job.setInputFormat(TextInputFormat.class);

		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(DataStructureWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out))
			fs.delete(out, true);

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		String line;
		int res = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
		System.out.println("");
		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] fss = fs.listStatus(new Path(args[1]));
		for (FileStatus status : fss) {
			Path path = status.getPath();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			line = br.readLine();
			while (line != null) {
				System.out.println(line);
				line = br.readLine();
			}
		}		
		System.exit(res);
	}

}
