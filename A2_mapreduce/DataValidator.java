import java.time.temporal.ChronoUnit;


public class DataValidator {

	/**
	 * method to check if the record read is good.
	 * 
	 * @param recordData
	 *            record read
	 * @return boolean
	 */
	public boolean isGoodRecord(RecordData recordData) {

		boolean goodRecord = true;
		// sanity checks to be performed
		if (!checkTimes(recordData) || !checkOrigDestData(recordData)
				|| !checkIDData(recordData)) {
			goodRecord = false;
		}
		// sanity checks for flights that are not Cancelled:
		if (!recordData.isCancelled()) {
			if (!checkNonCancelledData(recordData))
				goodRecord = false;
		}
		return goodRecord;
	}

	/**
	 * method to check conditions for non cancelled flights.
	 * 
	 * @param recordData
	 *            record read
	 * @return boolean
	 */
	private boolean checkNonCancelledData(RecordData recordData) {
		boolean dataGood = true;

		double timeDiff = ChronoUnit.MINUTES.between(
				recordData.getDepTime(), recordData.getArrTime());

		double timeZone = timeDiff - recordData.getActualElapsedTime();

		// ArrTime - DepTime - ActualElapsedTime - timeZone should be zero
		if ((timeDiff - recordData.getActualElapsedTime() - timeZone) != 0) {
			dataGood = false;
		}
		// if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
		if (recordData.getArrDelay() > 0) {
			if (recordData.getArrDelay() != recordData.getArrivalDelayNew())
				dataGood = false;
		}
		// if ArrDelay < 0 then ArrDelayMinutes should be zero
		if (recordData.getArrDelay() < 0) {
			if (recordData.getArrivalDelayNew() != 0)
				dataGood = false;
		}
		// if ArrDelayMinutes >= 15 then ArrDel15 should be false
		if (recordData.getArrivalDelayNew() >= 15) {
			if (recordData.getArrDel15() == 0)
				dataGood = false;
		}
		return dataGood;
	}

	/**
	 * method to check times.
	 * 
	 * @param recordData
	 *            record read
	 * @return boolean
	 */
	private boolean checkTimes(RecordData recordData) {
		boolean dataGood = true;

		// getting time difference in minutes
		double timeDiff = ChronoUnit.MINUTES.between(
				recordData.getCrsDepTime(), recordData.getCrsArrTime());

		double timeZone = timeDiff - recordData.getCrsElapsedTime();

		// timeZone % 60 should be 0
		if (timeZone % 60 != 0) {
			dataGood = false;
		}
		return dataGood;
	}

	/**
	 * method to check airport ids.
	 * 
	 * @param recordData
	 *            record read
	 * @return boolean
	 */
	private boolean checkIDData(RecordData recordData) {
		boolean dataGood = true;
		// AirportID, AirportSeqID, CityMarketID, StateFips, Wac should be
		// larger than 0
		if (recordData.getDestAirportId() <= 0
				|| recordData.getOriginAirportId() <= 0
				|| recordData.getDestAirportSeqId() <= 0
				|| recordData.getOriginAirportSeqId() <= 0
				|| recordData.getDestCityMArketId() <= 0
				|| recordData.getOriginCityMarketId() <= 0
				|| recordData.getDestStateFips() <= 0
				|| recordData.getOriginStateFips() <= 0
				|| recordData.getDestWac() <= 0
				|| recordData.getOriginWac() <= 0) {
			dataGood = false;
		}
		return dataGood;
	}

	/**
	 * method to check origin and destination data.
	 * 
	 * @param recordData
	 *            record read
	 * @return boolean
	 */
	private boolean checkOrigDestData(RecordData recordData) {
		boolean dataGood = true;
		// Origin, Destination, CityName, State, StateName should not be
		// empty
		if (recordData.getOrigin().isEmpty()
				|| recordData.getDest().isEmpty()
				|| recordData.getOriginCityName().isEmpty()
				|| recordData.getDestCityName().isEmpty()
				|| recordData.getOriginStateNm().isEmpty()
				|| recordData.getOriginStateAbr().isEmpty()
				|| recordData.getDestStateNm().isEmpty()
				|| recordData.getDestStateAbr().isEmpty()) {
			dataGood = false;
		}
		return dataGood;
	}
}
