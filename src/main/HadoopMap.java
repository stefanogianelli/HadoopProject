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
	private final static String logPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+) \"([^\"\\\\]*(?:\\\\.[^\"\\\\]*)*)\" \"([^\"]*)\".*";
	private final static String startDate = "22/Apr/2003";
	private final static String endDate = "30/May/2003";
	
	private Pattern p = Pattern.compile(logPattern);
	private String line, domain;
	private Matcher matcher;
	private long start = DateUtils.stringToDate(startDate);
	private long end = DateUtils.stringToDate(endDate);

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<DataStructureWritable, IntWritable> output,
			Reporter reporter) throws IOException {
		//get the line
		line = value.toString();
		//start matcher to divide the string in fields
		matcher = p.matcher(line);
		if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
			System.err.println("Bad log entry: " + line);
			return;
		}

		//get the date of the entry (group 4)
		long date = DateUtils.stringToDate(matcher.group(4).substring(0, 11));
		//get the request (group 5)
		String element = matcher.group(5);
		//get the referrer (group 8)
		String referrer = matcher.group(8).equals("-") ? null : matcher.group(8);
		//check if was made a request for the video
		if (element.contains("wmv")) {
			//emit output of type "video_downloads"
			output.collect(new DataStructureWritable(date, "video_downloads"),
					one);
			//check if the referrer is not null and it is between the start and the end date
			if (referrer != null && start <= date && date <= end) {
				//emit output of type "referrer"
				output.collect(new DataStructureWritable(date, "referrer"), one);
				//extract the domain of the referrer
				domain = this.getDomainName(referrer);
				//check if the domain was correctly detected
				if (domain != null) {
					//emit output of type "{name_of_the_domain}"
					output.collect(new DataStructureWritable(date, domain), one);
				}
			}
		} else {
			//emit output of type "page_views"
			output.collect(new DataStructureWritable(date, "page_views"), one);
		}
	}

	/**
	 * Extract the domain name from an URL
	 * @param url The url of the website
	 * @return The domain name extracted
	 */
	private String getDomainName(String url) {
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
