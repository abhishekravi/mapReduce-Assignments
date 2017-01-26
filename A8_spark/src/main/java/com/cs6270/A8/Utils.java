package com.cs6270.A8;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.opencsv.CSVParser;

/**
 * 
 * @author Chintan Pathak, Abhishek Ravi Chandran
 * @author Mania Abdi, Chinmayee Vaidya
 *
 */

/*
 * This class provides various utility functions to use in doing the required
 * processing
 */
public class Utils {

	final static CSVParser PARSER = new CSVParser();
	static String format = "yyyy-MM-dd";

	/**
	 * method to parse csv data.
	 * 
	 * @param value
	 *            csv data
	 * @return string array
	 */
	public static String[] parseCSV(String value) {
		String parsed[] = null;
		try {
			parsed = PARSER.parseLine(value);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return parsed;
	}

	/**
	 * method to get number of the week.
	 * 
	 * @param fdate
	 * date
	 * @return week number
	 */
	public static int getWeek(String fdate) {
		Calendar c = Calendar.getInstance();
		SimpleDateFormat df = new SimpleDateFormat(format);
		Date date = null;
		try {
			date = df.parse(fdate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c.setTime(date);
		return c.get(Calendar.WEEK_OF_YEAR);
	}

	/**
	 * method that uses quick select to calculate the median.
	 * 
	 * @param prices
	 *            list of prices
	 * @return median of the list
	 */

	public static float fastMedian(List<Float> values) {
		int arr_size = values.size();
		int mid_element = (arr_size / 2);
		float median = getMedian(values, mid_element, 0, arr_size);
		return median;
	}

	/**
	 * method to partition the array based on comparison of pivot.
	 * 
	 * @param values
	 *            array
	 * @param start
	 *            start pos of array
	 * @param end
	 *            end pos of array
	 * @return pivot position
	 */
	private static int partition(List<Float> values, int start, int end) {
		int left = start;
		int right = end - 1;
		int pivot = start;
		while (left < right) {
			if (pivot == left) {
				float current = values.get(pivot);
				float right_elem = values.get(right);
				if (right_elem > current) {
					right--;
				} else {
					values.set(right, current);
					values.set(pivot, right_elem);
					pivot = right;
				}
			} else {
				float current = values.get(pivot);
				float left_elem = values.get(left);
				if (left_elem <= current) {
					left++;
				}

				else {
					values.set(left, current);
					values.set(pivot, left_elem);
					pivot = left;
				}
			}
		}
		return pivot;
	}

	// median value to be calculated
	static float median = -1;

	/**
	 * method to get median using quick select.
	 * 
	 * @param values
	 *            array
	 * @param mid_element
	 *            middle element pos
	 * @param start
	 *            start pos
	 * @param end
	 *            end pos
	 * @return return the median value
	 */
	private static float getMedian(List<Float> values, int mid_element,
			int start, int end) {
		if (end - start < 1)
			return -1f;
		else if (end - start == 1)
			return values.get(start);

		int random_ind = start + (int) (Math.random() * (end - start));
		float temp = values.get(start);
		values.set(start, values.get(random_ind));
		values.set(random_ind, temp);
		int index = partition(values, start, end);
		if (index == mid_element) {
			median = values.get(index);
		} else if (index < mid_element) {
			median = getMedian(values, mid_element, index + 1, end);
		} else {

			median = getMedian(values, mid_element, start, index);
		}
		return median;
	}

}
