package main;

import graph.Graph;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.swing.JFrame;

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
import org.apache.log4j.BasicConfigurator;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.RefineryUtilities;

import data.DataStructureWritable;

public class HadoopDriver extends Configured implements Tool {

	private static Configuration conf = new Configuration();
	private static final String page_key = "page_views";
	private static final String page_label = "pageviews";
	private static final String video_key = "video_downloads";
	private static final String video_label = "video downloads";
	private static final String referer_key = "referrer";
	private static final String referer_label = "referrer";

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
		BasicConfigurator.configure();
		if (args[2].equals("1")) {
			conf.addResource(new Path(
					"/usr/local/hadoop/etc/hadoop/core-site.xml")
			);
			conf.addResource(new Path(
					"/usr/local/hadoop/etc/hadoop/hdfs-site.xml")
			);
		    conf.set("fs.hdfs.impl", 
	            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
	        );
	        conf.set("fs.file.impl",
	            org.apache.hadoop.fs.LocalFileSystem.class.getName()
	        );
		}
		String line;
		ToolRunner.run(conf, new HadoopDriver(), args);
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fss = fs.listStatus(new Path(args[1]));
		for (FileStatus status : fss) {
			Path path = status.getPath();
			Map<String, HashMap<String, Double>> list = new LinkedHashMap<String, HashMap<String, Double>>();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			line = br.readLine();
			while (line != null) {
				String[] split = line.split("\t");
				if (list.containsKey(split[0])) {
					list.get(split[0]).put(split[1],
							Double.parseDouble(split[2]));
				} else {
					HashMap<String, Double> val = new HashMap<String, Double>();
					val.put(split[1], Double.parseDouble(split[2]));
					list.put(split[0], val);
				}
				line = br.readLine();
			}

			for (Map.Entry<String, HashMap<String, Double>> entry : list
					.entrySet()) {
				if (entry.getValue().containsKey(page_key)) {
					dataset.addValue(entry.getValue().get(page_key)
							.doubleValue(), page_label, entry.getKey());
				} else {
					dataset.addValue(0.0, page_label, entry.getKey());
				}
				if (entry.getValue().containsKey(video_key)) {
					dataset.addValue(entry.getValue().get(video_key)
							.doubleValue(), video_label, entry.getKey());
				} else {
					dataset.addValue(0.0, video_label, entry.getKey());
				}
				if (entry.getValue().containsKey(referer_key)) {
					dataset.addValue(entry.getValue().get(referer_key)
							.doubleValue(), referer_label, entry.getKey());
				} else {
					dataset.addValue(0.0, referer_label, entry.getKey());
				}				
			}
		}
		System.out.println("Building graph ...");
		Graph g = new Graph(dataset);
		g.pack();
		RefineryUtilities.centerFrameOnScreen(g);		
		g.setVisible(true);
		g.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	}
}
