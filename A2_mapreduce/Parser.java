import java.io.IOException;
import java.time.LocalDateTime;

import com.opencsv.CSVParser;

public class Parser {

	/**
	 * method to parse csv data and get RecordData object
	 * 
	 * @param record
	 *            csv data
	 * @param recordData
	 *            RecordData object
	 * @throws NumberFormatException
	 * @throws BadDataException
	 */
	public void getRecordData(String record, RecordData recordData)
			throws NumberFormatException, BadDataException {
		String[] flightRecord = parseCSV(record);
		setRecordValues(recordData, flightRecord);
	}

	/**
	 * method to parse csv data.
	 * 
	 * @param value
	 *            csv data
	 * @return string array
	 */
	public String[] parseCSV(String value) {
		String parsed[] = null;
		try {
			parsed = new CSVParser().parseLine(value);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return parsed;
	}

	/**
	 * method to convert the record data into a pojo.
	 * 
	 * @param recordData
	 *            record object
	 * @param flightRecord
	 *            record array
	 * @throws BadDataException
	 * @throws NumberFormatException
	 */
	private void setRecordValues(RecordData recordData, String[] flightRecord)
			throws BadDataException, NumberFormatException {
		recordData.setYear(Integer.parseInt(flightRecord[0]));
		recordData.setMonth(Integer.parseInt(flightRecord[1]));
		recordData.setCarrier(flightRecord[8]);
		if (flightRecord[47].equals("0")) {
			recordData.setCancelled(false);
			recordData.setAvgTicketPrice(Double.parseDouble(flightRecord[109]));
			recordData.setActualElapsedTime(Integer.parseInt(flightRecord[51]));
			recordData.setArrDel15(Double.parseDouble(flightRecord[44]));
			recordData.setArrDelay(Double.parseDouble(flightRecord[42]));
			recordData.setArrivalDelayNew(Double.parseDouble(flightRecord[43]));
		} else {
			recordData.setCancelled(true);
		}
		settingDates(recordData, flightRecord);
		recordData.setCrsElapsedTime(Integer.parseInt(flightRecord[50]));
		recordData.setDest(flightRecord[23]);
		recordData.setDestAirportId(Integer.parseInt(flightRecord[20]));
		recordData.setDestAirportSeqId(Integer.parseInt(flightRecord[21]));
		recordData.setDestCityMArketId(Integer.parseInt(flightRecord[22]));
		recordData.setDestCityName(flightRecord[24]);
		recordData.setDestStateAbr(flightRecord[25]);
		recordData.setDestStateFips(Integer.parseInt(flightRecord[26]));
		recordData.setDestStateNm(flightRecord[27]);
		recordData.setDestWac(Integer.parseInt(flightRecord[28]));
		recordData.setOrigin(flightRecord[14]);
		recordData.setOriginAirportId(Integer.parseInt(flightRecord[11]));
		recordData.setOriginAirportSeqId(Integer.parseInt(flightRecord[12]));
		recordData.setOriginCityMarketId(Integer.parseInt(flightRecord[13]));
		recordData.setOriginCityName(flightRecord[15]);
		recordData.setOriginStateAbr(flightRecord[16]);
		recordData.setOriginStateFips(Integer.parseInt(flightRecord[17]));
		recordData.setOriginStateNm(flightRecord[18]);
		recordData.setOriginWac(Integer.parseInt(flightRecord[19]));
	}

	/**
	 * method to set the dates.
	 * 
	 * @param recordData
	 *            record object
	 * @param flightRecord
	 *            record map
	 * @throws Exception
	 */
	private void settingDates(RecordData recordData, String[] flightRecord)
			throws BadDataException {
		LocalDateTime depDate = LocalDateTime.now();
		LocalDateTime arrDate = LocalDateTime.now();
		if (!recordData.isCancelled()) {
			double arrTime = Double.parseDouble(flightRecord[41]);
			if (arrTime == 2400) {
				arrTime = 0000;
			}
			arrDate = arrDate.withHour((int) arrTime / 100);
			arrDate = arrDate.withMinute((int) arrTime % 100);
			arrDate = arrDate.withYear(recordData.getYear());
			arrDate = arrDate.withMonth(recordData.getMonth());

			double depTime = Double.parseDouble(flightRecord[30]);
			if (depTime == 2400) {
				depTime = 0000;
			}
			depDate = depDate.withHour((int) depTime / 100);
			depDate = depDate.withMinute((int) depTime % 100);
			depDate = depDate.withYear(recordData.getYear());
			depDate = depDate.withMonth(recordData.getMonth());

			if (arrTime < depTime) {
				arrDate = arrDate.plusDays(1);
			}
		}

		LocalDateTime cDepDate = LocalDateTime.now();
		LocalDateTime cArrDate = LocalDateTime.now();

		double cArrTime = Double.parseDouble(flightRecord[40]);
		double cDepTime = Double.parseDouble(flightRecord[29]);

		if (cArrTime == 0 || cDepTime == 0) {
			throw new BadDataException("time epmty");
		}
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

		recordData.setCrsArrTime(cArrDate);
		recordData.setCrsDepTime(cDepDate);
		recordData.setDepTime(depDate);
		recordData.setArrTime(arrDate);

	}
}
