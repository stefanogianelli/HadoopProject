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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
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
	private static final String intermediate = "/starwars/temp";

	@Override
	public int run(String[] arg0) throws Exception {
		JobConf jobConf1 = new JobConf(conf, HadoopDriver.class);
		jobConf1.setJobName("job1");

		Path in1 = new Path(arg0[0]);
		Path out1 = new Path(intermediate);
		FileInputFormat.setInputPaths(jobConf1, in1);
		FileOutputFormat.setOutputPath(jobConf1, out1);		

		jobConf1.setMapperClass(HadoopMap.class);
		jobConf1.setCombinerClass(HadoopReduce.class);
		jobConf1.setReducerClass(HadoopReduce.class);

		jobConf1.setInputFormat(TextInputFormat.class);

		jobConf1.setOutputFormat(TextOutputFormat.class);
		jobConf1.setOutputKeyClass(DataStructureWritable.class);
		jobConf1.setOutputValueClass(IntWritable.class);
		
		JobConf jobConf2 = new JobConf(conf, HadoopDriver.class);
		jobConf2.setJobName("job2");

		Path in2 = new Path(intermediate);
		Path out2 = new Path(arg0[1]);
		FileInputFormat.setInputPaths(jobConf2, in2);
		FileOutputFormat.setOutputPath(jobConf2, out2);		

		jobConf2.setMapperClass(ReferrerMap.class);
		jobConf2.setCombinerClass(ReferrerReduce.class);
		jobConf2.setReducerClass(ReferrerReduce.class);

		jobConf2.setInputFormat(TextInputFormat.class);

		jobConf2.setOutputFormat(TextOutputFormat.class);
		jobConf2.setOutputKeyClass(DataStructureWritable.class);
		jobConf2.setOutputValueClass(IntWritable.class);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out1))
			fs.delete(out1, true);
		if (fs.exists(out2))
			fs.delete(out2, true);
		
        Job job1 = new Job(jobConf1);
        Job job2 = new Job(jobConf2);
        JobControl jobControl = new JobControl("HadoopProject");
        jobControl.addJob(job1);
        jobControl.addJob(job2);
        job2.addDependingJob(job1);
        
        Thread t = new Thread(jobControl); 
        t.setDaemon(true);
        t.start(); 
                      
        while (!jobControl.allFinished()) { 
          try { 
            Thread.sleep(1000); 
          } catch (InterruptedException e) { 
            e.printStackTrace();
          } 
        } 

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
