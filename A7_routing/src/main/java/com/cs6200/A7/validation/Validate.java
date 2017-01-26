package com.cs6200.A7.validation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.cs6200.A7.data.App;
import com.cs6200.tools.Utils;
import com.opencsv.CSVReader;

/**
 * 
 * Class to validate the output.
 * 
 * @author Chinmayee Vaidya
 * @author Mania Abdi
 *
 */
public class Validate implements App {
	// map collects all the data from the validate file so that it can be
	// checked with the output
	static Map<String, Map<String, Set<String>>> map = new HashMap<String, Map<String, Set<String>>>();
	static int totalMins = 0, mismatch = 0;
	static int incorrect = 0;

	@Override
	public void runApp(Object... args) {
		String outputFolder = (String) args[0];
		String validate = (String) args[1];
		readDataFromValidate(validate);
		readDataFromOutput(outputFolder);
		StringBuilder sb = new StringBuilder();
		int correct = map.size() - incorrect;
		int total = map.size();
		sb.append("Number of Good connections chosen:").append((correct)).append("\n")
				.append("Number of missed connections chosen:").append(incorrect).append("\n")
				.append("Total itenaries:").append(total).append("\n").append("% of good connections chosen:")
				.append(((float) correct / (float) total) * 100).append("%").append("\n")
				.append("% of bad connections chosen:").append(((float) incorrect / (float) total) * 100).append("%");
		PrintWriter writer = null;
		try {
			writer = new PrintWriter("op.txt", "UTF-8");
		} catch (FileNotFoundException e) {
			System.err.println("File not found - " + e.getLocalizedMessage());
		} catch (UnsupportedEncodingException e) {
			System.err.println("Unsupported encoding exception - " + e.getLocalizedMessage());
		}
		writer.println("Total duration = " + totalMins);
		writer.println(sb.toString());
		writer.close();
	}

	/**
	 * 
	 * @param outputFolder
	 *            reads data from the output folder to see if the entry matches
	 *            with the data in file validate.
	 */
	private static void readDataFromOutput(String outputFolder) {
		File file = new File(outputFolder);
		File[] listFiles = file.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.getName().startsWith("part-r-");
			}
		});
		for (File f : listFiles) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(f));
				String str = "";
				while ((str = br.readLine()) != null) {
					String values[] = Utils.parseCSV(str);
					String year = values[0];
					String mnth = values[1];
					String day = values[2];
					String orig = values[3];
					String dest = values[4];
					String fl_orig = values[5];
					String fl_dest = values[6];
					String date = year + "-" + mnth + "-" + day;
					String orig_dest = orig + "+" + dest;
					String orig_dest_num = fl_orig + fl_dest;
					int duration = Integer.parseInt(values[7]);
					Map<String, Set<String>> vals = map.get(orig_dest);
					if (vals.containsKey(date)) {
						Set<String> set_flights = vals.get(date);
						// if we have chosen a missed connection
						if (set_flights.contains(orig_dest_num)) {
							incorrect++;
							totalMins += duration;
							totalMins += 100;
						} else {
							// connection chosen is not missed
							totalMins += duration;
						}
					}
				}
				br.close();
			} catch (IOException e) {
				System.err.println("IOException in readDataFromOutput with message - " + e.getLocalizedMessage());
			}
		}
	}

	/**
	 * reads data from the validate file and stores it in the map
	 * 
	 * @param validate
	 * @throws IOException
	 */
	private static void readDataFromValidate(String validate) {
		try {
			File dir = new File(validate);
			File[] listFiles = dir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pathname.getName().endsWith("csv.gz");
				}
			});
			InputStream is = new GZIPInputStream(new FileInputStream(listFiles[0]));
			BufferedReader b = new BufferedReader(new InputStreamReader(is));
			CSVReader reader = new CSVReader(b);
			String csvData[];
			while ((csvData = reader.readNext()) != null) {
				String year = csvData[0];
				String mnth = csvData[1];
				String day = csvData[2];
				String orig = csvData[3];
				String dest = csvData[4];
				String fl_orig = csvData[5];
				String fl_dest = csvData[6];
				String data = orig + "+" + dest;
				if (map.containsKey(data)) {
					Map<String, Set<String>> values = map.get(data);
					String date = year + "-" + mnth + "-" + day;
					if (values.containsKey(date)) {
						values.get(date).add(fl_orig + fl_dest);
					} else {
						Set<String> val = new HashSet<String>();
						val.add(fl_orig + fl_dest);
						values.put(date, val);
					}
					map.put(data, values);
				} else {
					String date = year + "-" + mnth + "-" + day;
					Set<String> val = new HashSet<String>();
					val.add(fl_orig + fl_dest);
					HashMap<String, Set<String>> vals = new HashMap<String, Set<String>>();
					vals.put(date, val);
					map.put(data, vals);
				}
			}
			reader.close();
		} catch (IOException e) {
			System.err.println("IOException in readDataFromValidate with message - " + e.getLocalizedMessage());
		}

	}
}
