package main;

import graph.BarChart;
import graph.Graph;

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
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

import utils.DateUtils;

import data.DataStructureWritable;

public class HadoopDriver extends Configured implements Tool {

	private static Configuration conf = new Configuration();
	private static final String page_key = "page_views";
	private static final String page_label = "pageviews";
	private static final String video_key = "video_downloads";
	private static final String video_label = "video downloads";
	private static final String referrer_key = "referrer";
	private static final String referrer_label = "referrer";

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
		LogManager.getRootLogger().setLevel(Level.INFO);
		
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
		ToolRunner.run(conf, new HadoopDriver(), args);
		System.out.println("Building dataset ...");
		buildDataset(args[1]);
		System.out.println("Finish");
	}
	
	private static void buildDataset (String output) throws Exception {
		String line;
		//create dataset for the graph		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fss = fs.listStatus(new Path(output));
		TimeSeries pageSerie = new TimeSeries(page_label);
		TimeSeries videoSerie = new TimeSeries(video_label);
		TimeSeries referrerSerie = new TimeSeries(referrer_label);
		DefaultCategoryDataset domainsDataset = new DefaultCategoryDataset();
		for (FileStatus status : fss) {
			Path path = status.getPath();
			Map<String, HashMap<String, Double>> list = new LinkedHashMap<String, HashMap<String, Double>>();
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
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
				String date = entry.getKey();
				for (Map.Entry<String, Double> item : entry.getValue().entrySet()) {
					if (item.getKey().equals(page_key)) {						
						pageSerie.add(DateUtils.stringToDay(entry.getKey()), item.getValue().doubleValue());
					} else if (item.getKey().equals(video_key)) {
						videoSerie.add(DateUtils.stringToDay(entry.getKey()), item.getValue().doubleValue());
					} else if (item.getKey().equals(referrer_key)) {
						referrerSerie.add(DateUtils.stringToDay(entry.getKey()), item.getValue().doubleValue());
					} else {
						domainsDataset.addValue(item.getValue().doubleValue(), item.getKey(), date);
					}
				}
			}
		}
		TimeSeriesCollection dataset = new TimeSeriesCollection();
		dataset.addSeries(pageSerie);
		dataset.addSeries(videoSerie);
		dataset.addSeries(referrerSerie);
		//create the graphs
		System.out.println("Building the line chart graph ...");
		createLineChart(dataset);
		System.out.println("Building the bar chart graph ...");
		createBarChart(domainsDataset);
	}
	
	private static void createLineChart (TimeSeriesCollection dataset) {
		//show the graph
		Graph g = new Graph(dataset);
		new Thread(g).start();
	}
	
	private static void createBarChart (CategoryDataset dataset) throws Exception {
		//show the graph
		BarChart g = new BarChart(dataset);
		new Thread(g).start();
	}
}
