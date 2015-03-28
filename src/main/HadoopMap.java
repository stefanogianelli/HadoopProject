package main;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import data.DataStructureWritable;

public class HadoopMap extends MapReduceBase implements
		Mapper<LongWritable, Text, DataStructureWritable, IntWritable> {

	private Text text = new Text();
	private final static IntWritable one = new IntWritable(1);
	private final static int NUM_FIELDS = 9;
	private final static String logPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
	private Pattern p = Pattern.compile(logPattern);
	private String line;
	private Matcher matcher;

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<DataStructureWritable, IntWritable> output,
			Reporter reporter) throws IOException {
		line = value.toString();
		matcher = p.matcher(line);
		if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
			System.err.println("Bad log entry: " + line);
		} else {
			String date = matcher.group(4).substring(0, 11);
			String element = matcher.group(5);
			String referer = matcher.group(8).equals("-") ? null : matcher
					.group(8);
			if (element.contains("wmv")) {
				text.set("video_download");
				output.collect(new DataStructureWritable(date,
						"video_downloads"), one);
				if (referer != null) {
					output.collect(new DataStructureWritable(date, "referer"),
							one);
					try {
						output.collect(
								new DataStructureWritable(date, this
										.getDomainName(referer)), one);
					} catch (URISyntaxException e) {
						System.err.println("Domain parsing failed");
					}
				}
			} else if (element.contains("html")) {
				text.set("web_pages");
				output.collect(new DataStructureWritable(date, "page_views"),
						one);
				if (referer != null) {
					output.collect(new DataStructureWritable(date, "referer"),
							one);
					try {
						output.collect(
								new DataStructureWritable(date, this
										.getDomainName(referer)), one);
					} catch (URISyntaxException e) {
						System.err.println("Domain parsing failed");
					}
				}
			}
		}
	}

	public String getDomainName(String url) throws URISyntaxException {
		URI uri = new URI(url);
		String domain = uri.getHost();
		return domain.startsWith("www.") ? domain.substring(4) : domain;
	}

}
