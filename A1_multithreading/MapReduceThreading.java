import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import com.opencsv.CSVReader;

/**
 * 
 * @author Abhishek Ravi Chandran
 *	File processing with Threads
 */
public class MapReduceThreading {

	// Data variable that will be shared by all threads.
	final static Data DATA = new Data();

	/**
	 * main method.
	 * 
	 * @param args
	 *            -p	parallel processing
	 *            -DIR= Directory path
	 */

	public static void main(String[] args) {
		
		//holds the directory path
		String dirPath = getDirPath(args);
		// get list of files
		List<String> files = processDir(dirPath);
		
		//number of threads equals to number of files
		final int THREADPOOL = files.size();
		
		Thread[] threads = new Thread[THREADPOOL];
		
		// start a thread to process each file
		for (int i = 0; i < THREADPOOL; i++) {
			threads[i] = new Thread(new Processor(files.get(i), i));
		}

		//start all the threads
		for (Thread t : threads) {
			t.start();
		}

		//all threads will join the main thread once they are finished
		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//calculate average and median
		calculateAverage(DATA);
		
		//method to sort the data according to mean
		DATA.records = sortData(DATA.records);
		
		//printing the output to the console
		System.out.println(DATA.knum);
		System.out.println(DATA.fnum);
		for (Entry<String, Carrier> k : DATA.records.entrySet()) {
			System.out.println(k.getKey() + " "
					+ round(k.getValue().meanPrice, 2) + " "
					+ round(k.getValue().median, 2));
		}
	}

	/**
	 * method to get dir path.
	 * 
	 * @param args
	 *            command line arduments
	 * @return dir path
	 */
	private static String getDirPath(String[] args) {
		String dir = "";
		for (String arg : args) {
			if (arg.contains("-DIR")) {
				dir = arg.split("=")[1];
			}
		}
		return dir;
	}

	/**
	 * method to calculate the average and median.
	 * @param data
	 * flight data
	 */
	private static void calculateAverage(Data data) {
		for (String k : data.records.keySet()) {
			//mean = total / number of records
			data.records.get(k).meanPrice = data.records.get(k).totalPrice
					/ data.records.get(k).count;
			
			//getting median by sorting the prices and picking the middle value
			Collections.sort(data.records.get(k).prices);
			data.records.get(k).median = data.records.get(k).prices
					.get(data.records.get(k).prices.size() / 2 - 10);
		}
	}

	/**
	 * method to read the directory and get the list of all files.
	 * @param dirPath
	 * directory path
	 * @return
	 * list of file names with path
	 */
	private static List<String> processDir(String dirPath) {
		// get all filename in the directory
		List<String> files = new ArrayList<String>();
		try {
			Files.walk(Paths.get(dirPath)).forEach(filePath -> {
				if (Files.isRegularFile(filePath)) {
					files.add(filePath.toString());
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		return files;
	}

	/**
	 * method to sort the map data according to the carrier average prices.
	 * 
	 * @param input
	 *            unsorted map
	 * @return sorted map
	 */
	private static Map<String, Carrier> sortData(Map<String, Carrier> input) {

		// Convert Map to List
		List<Map.Entry<String, Carrier>> list = new LinkedList<Map.Entry<String, Carrier>>(
				input.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Carrier>>() {
			public int compare(Map.Entry<String, Carrier> o1,
					Map.Entry<String, Carrier> o2) {
				return (Double.valueOf(o1.getValue().meanPrice)).compareTo(o2
						.getValue().meanPrice);
			}
		});

		// Convert sorted map back to a Map
		Map<String, Carrier> sortedMap = new LinkedHashMap<String, Carrier>();
		for (Iterator<Map.Entry<String, Carrier>> it = list.iterator(); it
				.hasNext();) {
			Map.Entry<String, Carrier> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	/**
	 * util method to round double values to any precision.
	 * 
	 * @param value
	 *            value
	 * @param places
	 *            places to round to
	 * @return double
	 */
	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		BigDecimal bd = new BigDecimal(value);
		bd = bd.setScale(places, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}

	/**
	 * 
	 * @author Abhishek Ravi Chandran class to hold the airline data.
	 *
	 */
	static class Data {
		Data() {
			this.records = new HashMap<String, Carrier>();
		}

		// contain all the records in a map with the carrier as the key
		Map<String, Carrier> records;
		// holding all the headers
		String headers[];
		int total;
		int knum;
		int fnum;
		int blen;
		int selected;
	}

	/**
	 * 
	 * @author Abhishek Ravi Chandran class to hold data for individual carriers
	 *
	 */
	static class Carrier {

		Carrier() {
			this.prices = new ArrayList<Double>();
		}

		// name of the carrier
		String carrier;
		// number of flights
		int count;
		// list of prices
		List<Double> prices;
		// total price
		double totalPrice;
		// mean price for this carrier
		double meanPrice;
		// median price for this carrier
		double median;
	}

	/**
	 * Thread class that will process each file.
	 * @author Abhishek Ravi Chandran
	 *
	 */
	static class Processor implements Runnable {
		//file to process
		String file;
		//thread id
		int id;

		Processor(String file, int id) {
			this.file = file;
			this.id = id;
		}

		@Override
		public void run() {
			processData(this.file);
		}

		/**
		 * method to process the airline data.
		 * 
		 * @param fileName
		 *            name of the file to be processed
		 */
		public void processData(String fileName) {
			InputStream fileStream;
			Reader decoder;
			try {
				// file to be read
				fileStream = new FileInputStream(fileName);
				// creating gzip stream to read the gzip file
				InputStream gzipStream = new GZIPInputStream(fileStream);
				// defining the encoding
				decoder = new InputStreamReader(gzipStream, "UTF-8");
				BufferedReader buffer = new BufferedReader(decoder);
				CSVReader reader = new CSVReader(buffer);
				String[] record;
				// getting the headers from the file from line 1
				record = reader.readNext();
				Data localData = new Data();
				localData.headers = Arrays.copyOfRange(record, 0, 110);
				// incrementing knum as this is a bad line
				localData.knum++;
				localData.total++;

				// reading the data from the file, line by line
				while ((record = reader.readNext()) != null) {
					localData.total++;
					processRecord(record, localData);
				}
				reader.close();
				buffer.close();
				//updating the global data variable
				commonUpdate(localData);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		 * method to update the common global variable by acquiring a lock to be thread safe.
		 * @param localData
		 * data gathered by this thread
		 */
		synchronized void commonUpdate(Data localData) {
			synchronized (DATA) {
				DATA.knum += localData.knum;
				DATA.fnum += localData.fnum;
				DATA.total += localData.total;
				for (String k : localData.records.keySet()) {
					if (!localData.records.get(k).prices.isEmpty()) {
						if (DATA.records.get(k) == null) {
							DATA.records.put(k, localData.records.get(k));
						} else {
							DATA.records.get(k).count += localData.records
									.get(k).count;
							DATA.records.get(k).totalPrice += localData.records
									.get(k).totalPrice;
							DATA.records.get(k).prices.addAll(localData.records
									.get(k).prices);
						}
					}
				}
			}
		}

		/**
		 * method to process each record.
		 * 
		 * @param record
		 *            line that was read
		 * @param data
		 *            Object to store the processed data
		 */
		private void processRecord(String[] record, Data data) {
			// corrupt lines

			if (record.length > data.headers.length) {
				data.knum++;
				data.blen++;
			} else {
				String carrier = record[8];
				if (data.records.get(carrier) == null)
					data.records.put(carrier, new Carrier());
				Carrier carr = data.records.get(carrier);
				carr.carrier = carrier;
				// putting data into a map for easy access and easy code
				// readability
				Map<String, String> flightRecord = new HashMap<String, String>();
				for (int i = 0; i < record.length; i++) {
					flightRecord.put(data.headers[i], record[i]);
				}
				double year = Double.parseDouble(flightRecord.get("YEAR"));
				double month = Double.parseDouble(flightRecord.get("MONTH"));
				if (isGoodRecord(flightRecord)) {
					//filtering and fetching records dated JAN 2015
					if (year == 2015
							&& month == 1
							&& Double
									.parseDouble(flightRecord.get("CANCELLED")) == 0) {
						double avgPrice = Double.parseDouble(flightRecord
								.get("AVG_TICKET_PRICE"));
						if (avgPrice > 0) {
							carr.count++;
							carr.prices.add(avgPrice);
							carr.totalPrice += avgPrice;
						}
					}
					data.fnum++;

				} else {
					data.knum++;
				}
			}
		}

		/**
		 * method to check if the record read is good.
		 * 
		 * @param flightRecord
		 *            record read
		 * @return boolean
		 */
		private boolean isGoodRecord(Map<String, String> flightRecord) {

			boolean goodRecord = true;
			// sanity checks to be performed
			if (!checkTimes(flightRecord) || !checkOrigDestData(flightRecord)
					|| !checkIDData(flightRecord)) {
				goodRecord = false;
			}
			// sanity checks for flights that are not Cancelled:
			if (Double.parseDouble(flightRecord.get("CANCELLED")) == 0) {
				if (!checkNonCancelledData(flightRecord))
					goodRecord = false;
			}
			return goodRecord;
		}

		/**
		 * method to check conditions for non cancelled flights.
		 * 
		 * @param flightRecord
		 *            record read
		 * @return boolean
		 */
		private boolean checkNonCancelledData(Map<String, String> flightRecord) {
			boolean dataGood = true;
			LocalDateTime depDate = LocalDateTime.now();
			LocalDateTime arrDate = LocalDateTime.now();

			double arrTime = Double.parseDouble(flightRecord.get("ARR_TIME"));
			if (arrTime == 2400) {
				arrTime = 0000;
			}
			arrDate = arrDate.withHour((int) arrTime / 100);
			arrDate = arrDate.withMinute((int) arrTime % 100);

			double depTime = Double.parseDouble(flightRecord.get("DEP_TIME"));
			if (depTime == 2400) {
				depTime = 0000;
			}
			depDate = depDate.withHour((int) depTime / 100);
			depDate = depDate.withMinute((int) depTime % 100);

			if (arrTime < depTime) {
				arrDate = arrDate.plusDays(1);
			}

			double timeDiff = ChronoUnit.MINUTES.between(depDate, arrDate);

			double arrDelay = Double.parseDouble(flightRecord.get("ARR_DELAY"));
			double actElapsedTime = Double.parseDouble(flightRecord
					.get("ACTUAL_ELAPSED_TIME"));
			double arrDelayMinutes = Double.parseDouble(flightRecord
					.get("ARR_DELAY_NEW"));
			double timeZone = timeDiff - actElapsedTime;

			// ArrTime - DepTime - ActualElapsedTime - timeZone should be zero
			if ((timeDiff - actElapsedTime - timeZone) != 0) {
				dataGood = false;
			}
			// if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
			if (arrDelay > 0) {
				if (arrDelay != arrDelayMinutes)
					dataGood = false;
			}
			// if ArrDelay < 0 then ArrDelayMinutes should be zero
			if (arrDelay < 0) {
				if (arrDelayMinutes != 0)
					dataGood = false;
			}
			// if ArrDelayMinutes >= 15 then ArrDel15 should be false
			if (arrDelayMinutes >= 15) {
				if (Double.parseDouble(flightRecord.get("ARR_DEL15")) == 0)
					dataGood = false;
			}
			return dataGood;
		}

		/**
		 * method to check times.
		 * 
		 * @param flightRecord
		 *            record read
		 * @return boolean
		 */
		private boolean checkTimes(Map<String, String> flightRecord) {
			boolean dataGood = true;
			LocalDateTime cDepDate = LocalDateTime.now();
			LocalDateTime cArrDate = LocalDateTime.now();

			double cArrTime = Double.parseDouble(flightRecord
					.get("CRS_ARR_TIME"));
			double cDepTime = Double.parseDouble(flightRecord
					.get("CRS_DEP_TIME"));

			// CRSArrTime and CRSDepTime should not be zero
			if (cArrTime == 0 || cDepTime == 0) {
				dataGood = false;
			} else {

				// converting to 00:00 standard time
				if (cArrTime == 2400) {
					cArrTime = 0000;
				}
				// converting to 00:00 standard time
				if (cDepTime == 2400) {
					cDepTime = 0000;
				}

				cArrDate = cArrDate.withHour((int) cArrTime / 100);
				cArrDate = cArrDate.withMinute((int) cArrTime % 100);

				cDepDate = cDepDate.withHour((int) cDepTime / 100);
				cDepDate = cDepDate.withMinute((int) cDepTime % 100);

				// incrementing by 1 day if the arrival time is the next day
				if (cArrTime < cDepTime) {
					cArrDate = cArrDate.plusDays(1);
				}

				// getting time difference in minutes
				double timeDiff = ChronoUnit.MINUTES
						.between(cDepDate, cArrDate);

				double timeZone = timeDiff
						- Double.parseDouble(flightRecord
								.get("CRS_ELAPSED_TIME"));

				// timeZone % 60 should be 0
				if (timeZone % 60 != 0) {
					dataGood = false;
				}
			}
			return dataGood;
		}

		/**
		 * method to check airport ids.
		 * 
		 * @param flightRecord
		 *            record read
		 * @return boolean
		 */
		private boolean checkIDData(Map<String, String> flightRecord) {
			boolean dataGood = true;
			// AirportID, AirportSeqID, CityMarketID, StateFips, Wac should be
			// larger than 0
			if (Double.parseDouble(flightRecord.get("DEST_AIRPORT_ID")) <= 0
					|| Double
							.parseDouble(flightRecord.get("ORIGIN_AIRPORT_ID")) <= 0
					|| Double.parseDouble(flightRecord
							.get("DEST_AIRPORT_SEQ_ID")) <= 0
					|| Double.parseDouble(flightRecord
							.get("ORIGIN_AIRPORT_SEQ_ID")) <= 0
					|| Double.parseDouble(flightRecord
							.get("DEST_CITY_MARKET_ID")) <= 0
					|| Double.parseDouble(flightRecord
							.get("ORIGIN_CITY_MARKET_ID")) <= 0
					|| Double.parseDouble(flightRecord.get("DEST_STATE_FIPS")
							.replace("\"", "")) <= 0
					|| Double.parseDouble(flightRecord.get("ORIGIN_STATE_FIPS")
							.replace("\"", "")) <= 0
					|| Double.parseDouble(flightRecord.get("DEST_WAC")) <= 0
					|| Double.parseDouble(flightRecord.get("ORIGIN_WAC")) <= 0) {
				dataGood = false;
			}
			return dataGood;
		}

		/**
		 * method to check origin and destination data.
		 * 
		 * @param flightRecord
		 *            record read
		 * @return boolean
		 */
		private boolean checkOrigDestData(Map<String, String> flightRecord) {
			boolean dataGood = true;
			// Origin, Destination, CityName, State, StateName should not be
			// empty
			if (flightRecord.get("ORIGIN").isEmpty()
					|| flightRecord.get("DEST").isEmpty()
					|| flightRecord.get("ORIGIN_CITY_NAME").isEmpty()
					|| flightRecord.get("DEST_CITY_NAME").isEmpty()
					|| flightRecord.get("ORIGIN_STATE_NM").isEmpty()
					|| flightRecord.get("ORIGIN_STATE_ABR").isEmpty()
					|| flightRecord.get("ORIGIN_STATE_FIPS").isEmpty()
					|| flightRecord.get("DEST_STATE_NM").isEmpty()
					|| flightRecord.get("DEST_STATE_ABR").isEmpty()
					|| flightRecord.get("DEST_STATE_FIPS").isEmpty()) {
				dataGood = false;
			}
			return dataGood;
		}

	}

}
