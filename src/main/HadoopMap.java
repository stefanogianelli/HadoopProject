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

import utils.DateUtils;
import data.DataStructureWritable;

public class HadoopMap extends MapReduceBase implements
		Mapper<LongWritable, Text, DataStructureWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private final static int NUM_FIELDS = 9;
	private final static String logPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
	private Pattern p = Pattern.compile(logPattern);
	private final static String logPatternAlt = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+) \"([^\"]+)\" \"([^\"]+)\"";
	private Pattern p1 = Pattern.compile(logPatternAlt);
	private String line, domain;
	private Matcher matcher;
	private long start = DateUtils.stringToDate("22/Apr/2003");
	private long end = DateUtils.stringToDate("30/May/2003");

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<DataStructureWritable, IntWritable> output,
			Reporter reporter) throws IOException {
		line = value.toString();
		matcher = p.matcher(line);
		if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
			matcher = p1.matcher(line);
			if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
				System.err.println("Bad log entry: " + line);
				return;
			}
		}

		long date = DateUtils.stringToDate(matcher.group(4).substring(0, 11));
		String element = matcher.group(5);
		String referer = matcher.group(8).equals("-") ? null : matcher.group(8);
		if (element.contains("wmv")) {
			output.collect(new DataStructureWritable(date, "video_downloads"),
					one);
			if (referer != null && start <= date && date <= end) {
				output.collect(new DataStructureWritable(date, "referer"), one);
				domain = this.getDomainName(referer);
				if (domain != null)
					output.collect(new DataStructureWritable(date, domain), one);
			}
		} else {
			output.collect(new DataStructureWritable(date, "page_views"), one);
		}
	}

	public String getDomainName(String url) {
		URI uri;
		try {
			uri = new URI(url);
		} catch (URISyntaxException e) {
			return null;
		}
		String domain = uri.getHost();
		if (domain != null)
			return domain.startsWith("www.") ? domain.substring(4) : domain;
		else
			return null;
	}

}
