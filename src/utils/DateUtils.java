package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

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
	
	public static String dateToString (long date) {
		SimpleDateFormat parser = new SimpleDateFormat(outputPattern);
		return parser.format(date);
	}

}
