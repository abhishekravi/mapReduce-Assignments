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
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
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

/**
 * 
 * @author Abhishek Ravi Chandran
 *
 */
public class MapReduceInhale {

	/**
	 * main method
	 * 
	 * @param args
	 * filename
	 */
	public static void main(String[] args) {
		MapReduceInhale mri = new MapReduceInhale();
		mri.processData(args[0]);
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
			// defineing the encoding
			decoder = new InputStreamReader(gzipStream, "UTF-8");
			BufferedReader buffer = new BufferedReader(decoder);
			String line = "";

			// getting the headers from the file from line 1
			line = buffer.readLine();
			// getting the headers by splitting the string by ','
			String headers[] = line.split(",");
			Data data = new Data(headers.length);
			data.headers = headers;
			// incrementing knum as this is a bad line
			data.knum++;

			// reading the data from the file, line by line
			while ((line = buffer.readLine()) != null) {
				processRecord(line, data);
			}

			// sorting the carrier data in increasing order of mean price
			data.records = sortData(data.records);
			System.out.println(data.knum);
			System.out.println(data.fnum);
			for (Entry<String, Carrier> k : data.records.entrySet()) {
				System.out.println(k.getKey() + " " + round(k.getValue().meanPrice, 2));
			}
			buffer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * method to sort the map data according to the carrier average prices.
	 * 
	 * @param input
	 *            unsorted map
	 * @return sorted map
	 */
	private Map<String, Carrier> sortData(Map<String, Carrier> input) {

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
	 * method to process each record.
	 * 
	 * @param line
	 *            line that was read
	 * @param data
	 *            Object to store the processed data
	 */
	private void processRecord(String line, Data data) {
		// split by , only if there are 0 or even number of quotes(")
		String cols[] = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		// corrupt lines
		if (cols.length > data.headers.length) {
			data.knum++;
			data.blen++;
		} else {
			String carrier = cols[8].replace("\"", "");
			if (data.records.get(carrier) == null)
				data.records.put(carrier, new Carrier());
			Carrier carr = data.records.get(carrier);
			carr.carrier = carrier;
			// putting data into a map for easy access and easy code readability
			Map<String, String> flightRecord = new HashMap<String, String>();
			for (int i = 0; i < cols.length; i++) {
				flightRecord.put(data.headers[i], cols[i]);
			}

			if (isGoodRecord(flightRecord)) {
				carr.count++;
				carr.totalPrice += Double.parseDouble(flightRecord
						.get("AVG_TICKET_PRICE"));
				// recalculating mean when any record is added
				carr.meanPrice = carr.totalPrice / carr.count;
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

		double arrTime = Double.parseDouble(flightRecord.get("ARR_TIME")
				.replace("\"", ""));
		if (arrTime == 2400) {
			arrTime = 0000;
		}
		arrDate = arrDate.withHour((int) arrTime / 100);
		arrDate = arrDate.withMinute((int) arrTime % 100);

		double depTime = Double.parseDouble(flightRecord.get("DEP_TIME")
				.replace("\"", ""));
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

		double cArrTime = Double.parseDouble(flightRecord.get("CRS_ARR_TIME")
				.replace("\"", ""));
		double cDepTime = Double.parseDouble(flightRecord.get("CRS_DEP_TIME")
				.replace("\"", ""));

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
			double timeDiff = ChronoUnit.MINUTES.between(cDepDate, cArrDate);

			double timeZone = timeDiff
					- Double.parseDouble(flightRecord.get("CRS_ELAPSED_TIME"));

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
				|| Double.parseDouble(flightRecord.get("ORIGIN_AIRPORT_ID")) <= 0
				|| Double.parseDouble(flightRecord.get("DEST_AIRPORT_SEQ_ID")) <= 0
				|| Double
						.parseDouble(flightRecord.get("ORIGIN_AIRPORT_SEQ_ID")) <= 0
				|| Double.parseDouble(flightRecord.get("DEST_CITY_MARKET_ID")) <= 0
				|| Double
						.parseDouble(flightRecord.get("ORIGIN_CITY_MARKET_ID")) <= 0
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
		// Origin, Destination, CityName, State, StateName should not be empty
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
	class Data {
		Data(int size) {
			this.headers = new String[size];
			this.records = new HashMap<String, Carrier>();
		}

		// contain all the records in a map with the carrier as the key
		Map<String, Carrier> records;
		// holding all the headers
		String headers[];
		int knum;
		int fnum;
		int blen;
	}

	/**
	 * 
	 * @author Abhishek Ravi Chandran class to hold data for individual carriers
	 *
	 */
	class Carrier {

		// name of the carrier
		String carrier;
		// number of flights
		int count;
		// total price
		double totalPrice;
		// mean price for this carrier
		double meanPrice;
	}

}
