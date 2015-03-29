package main;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

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
	
	private static Configuration conf = new Configuration();

	@Override
	public int run(String[] arg0) throws Exception {		
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
		if (args[2].equals("1")) {
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
		}
		String line;
		ToolRunner.run(conf, new HadoopDriver(), args);
		System.out.println("");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fss = fs.listStatus(new Path(args[1]));
		for (FileStatus status : fss) {
			Path path = status.getPath();
			Map<String, HashMap<String,Double>> list = new LinkedHashMap<String, HashMap<String,Double>>();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			line = br.readLine();
			while (line != null) {
				String [] split = line.split("\t");
				if (list.containsKey(split[0])) {
					list.get(split[0]).put(split[1], Double.parseDouble(split[2]));
				} else {
					HashMap<String,Double> val = new HashMap<String,Double>();
					val.put(split[1], Double.parseDouble(split[2]));
					list.put(split[0], val);
				}
				line = br.readLine();
			}
			int points_num = list.size();
			double [] pages = new double[points_num];
			double [] videos = new double[points_num];
			String [] labels = new String[points_num];
			int cont = 0;
			for (Map.Entry<String, HashMap<String,Double>> entry : list.entrySet()) {
				labels[cont] = entry.getKey();
				if (entry.getValue().containsKey("page_views")) {
					pages[cont] = entry.getValue().get("page_views").doubleValue();
				} else {
					pages[cont] = 0;
				}
				if (entry.getValue().containsKey("video_downloads")) {
					videos[cont] = entry.getValue().get("video_downloads").doubleValue();
				} else {
					videos[cont] = 0;
				}
				System.out.println("Data: " + labels[cont]);
				System.out.println("\tPageviews: " + pages[cont]);
				System.out.println("\tVideo downloads: " + videos[cont]);
				cont++;
			}
		}	
		System.exit(0);
	}

}
