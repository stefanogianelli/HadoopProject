package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.jfree.data.time.Day;

public class DateUtils {
	
	private static String inputPattern = "dd/MMM/yyyy";
	private static String outputPattern = "dd/MM/yyyy";
	
	public static long stringToDate (String date) {
		SimpleDateFormat parser = new SimpleDateFormat(inputPattern);
		try {
			return parser.parse(date).getTime();
		} catch (ParseException e) {
			return -1;
		}
	}
	
	public static Day stringToDay (String date) {
		SimpleDateFormat parser = new SimpleDateFormat(outputPattern);
		try {
			return new Day(parser.parse(date));
		} catch (ParseException e) {
			return null;
		}
	}
	
	public static String dateToString (long date) {
		SimpleDateFormat parser = new SimpleDateFormat(outputPattern);
		return parser.format(date);
	}

}
