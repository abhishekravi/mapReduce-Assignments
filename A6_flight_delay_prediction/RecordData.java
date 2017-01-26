import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.serializer.WritableSerialization;

/**
 * This class will hold a record data.
 * 
 * @author Abhishek Ravi Chandran
 * @author Chinmayee Vaidhya
 * 
 *
 */
public class RecordData extends WritableSerialization{

	int year;
	int month;
	int day;
	int quarter;
	int distance;
	int week;
	String fnum;
	String fldate;
	boolean cancelled;
	float price;
	Date arrTime;
	Date depTime;
	float arrDelay;
	int actualElapsedTime;
	float arrivalDelayNew;
	float arrDel15;
	Date crsArrTime;
	Date crsDepTime;
	int cat;
	int cdt;
	int crsElapsedTime;
	int destAirportId;
	int originAirportId;
	int destAirportSeqId;
	int originAirportSeqId;
	int destCityMArketId;
	int originCityMarketId;
	int destStateFips;
	int OriginStateFips;
	int DestWac;
	int OriginWac;
	String origin;
	String dest;
	String originCityName;
	String destCityName;
	String originStateNm;
	String originStateAbr;
	String destStateNm;
	String destStateAbr;
	String carrier;

	public String getCarrier() {
		return carrier;
	}

	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int i) {
		this.year = i;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public boolean isCancelled() {
		return cancelled;
	}

	public void setCancelled(boolean cancelled) {
		this.cancelled = cancelled;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	public Date getArrTime() {
		return arrTime;
	}

	public void setArrTime(Date arrTime) {
		this.arrTime = arrTime;
	}

	public Date getDepTime() {
		return depTime;
	}

	public void setDepTime(Date depTime) {
		this.depTime = depTime;
	}

	public float getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(float arrDelay) {
		this.arrDelay = arrDelay;
	}

	public int getActualElapsedTime() {
		return actualElapsedTime;
	}

	public void setActualElapsedTime(int actualElapsedTime) {
		this.actualElapsedTime = actualElapsedTime;
	}

	public double getArrivalDelayNew() {
		return arrivalDelayNew;
	}

	public void setArrivalDelayNew(float arrivalDelayNew) {
		this.arrivalDelayNew = arrivalDelayNew;
	}

	public float getArrDel15() {
		return arrDel15;
	}

	public void setArrDel15(float arrDel15) {
		this.arrDel15 = arrDel15;
	}

	public Date getCrsArrTime() {
		return crsArrTime;
	}

	public void setCrsArrTime(Date crsArrTime) {
		this.crsArrTime = crsArrTime;
	}

	public Date getCrsDepTime() {
		return crsDepTime;
	}

	public void setCrsDepTime(Date crsDepTime) {
		this.crsDepTime = crsDepTime;
	}

	public int getCrsElapsedTime() {
		return crsElapsedTime;
	}

	public void setCrsElapsedTime(int crsElapsedTime) {
		this.crsElapsedTime = crsElapsedTime;
	}

	public int getDestAirportId() {
		return destAirportId;
	}

	public void setDestAirportId(int destAirportId) {
		this.destAirportId = destAirportId;
	}

	public int getOriginAirportId() {
		return originAirportId;
	}

	public void setOriginAirportId(int originAirportId) {
		this.originAirportId = originAirportId;
	}

	public int getDestAirportSeqId() {
		return destAirportSeqId;
	}

	public void setDestAirportSeqId(int destAirportSeqId) {
		this.destAirportSeqId = destAirportSeqId;
	}

	public int getOriginAirportSeqId() {
		return originAirportSeqId;
	}

	public void setOriginAirportSeqId(int originAirportSeqId) {
		this.originAirportSeqId = originAirportSeqId;
	}

	public int getDestCityMArketId() {
		return destCityMArketId;
	}

	public void setDestCityMArketId(int destCityMArketId) {
		this.destCityMArketId = destCityMArketId;
	}

	public int getOriginCityMarketId() {
		return originCityMarketId;
	}

	public void setOriginCityMarketId(int originCityMarketId) {
		this.originCityMarketId = originCityMarketId;
	}

	public int getDestStateFips() {
		return destStateFips;
	}

	public void setDestStateFips(int destStateFips) {
		this.destStateFips = destStateFips;
	}

	public int getOriginStateFips() {
		return OriginStateFips;
	}

	public void setOriginStateFips(int originStateFips) {
		OriginStateFips = originStateFips;
	}

	public int getDestWac() {
		return DestWac;
	}

	public void setDestWac(int destWac) {
		DestWac = destWac;
	}

	public int getOriginWac() {
		return OriginWac;
	}

	public void setOriginWac(int originWac) {
		OriginWac = originWac;
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	public String getOriginCityName() {
		return originCityName;
	}

	public void setOriginCityName(String originCityName) {
		this.originCityName = originCityName;
	}

	public String getDestCityName() {
		return destCityName;
	}

	public void setDestCityName(String destCityName) {
		this.destCityName = destCityName;
	}

	public String getOriginStateNm() {
		return originStateNm;
	}

	public void setOriginStateNm(String originStateNm) {
		this.originStateNm = originStateNm;
	}

	public String getOriginStateAbr() {
		return originStateAbr;
	}

	public void setOriginStateAbr(String originStateAbr) {
		this.originStateAbr = originStateAbr;
	}

	public String getDestStateNm() {
		return destStateNm;
	}

	public void setDestStateNm(String destStateNm) {
		this.destStateNm = destStateNm;
	}

	public String getDestStateAbr() {
		return destStateAbr;
	}

	public void setDestStateAbr(String destStateAbr) {
		this.destStateAbr = destStateAbr;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}
	
	/**
	 * method to check if the record read is good.
	 * 
	 * @param recordData
	 *            record read
	 * @return boolean
	 */
	public  boolean isSane() {

		boolean goodRecord = true;
		// sanity checks to be performed
		if (!checkTimes() || !checkOrigDestData()
				|| !checkIDData()) {
			return false;
		}
		// sanity checks for flights that are not Cancelled:
		if (!this.isCancelled()) {
			if (!checkNonCancelledData())
				return false;
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
	private boolean checkNonCancelledData() {
		boolean dataGood = true;
		long duration  = this.getArrTime().getTime()
				- this.getDepTime().getTime();
		long timeDiff = TimeUnit.MILLISECONDS.toMinutes(duration);

		double timeZone = timeDiff - this.getActualElapsedTime();

		// ArrTime - DepTime - ActualElapsedTime - timeZone should be zero
		if ((timeDiff - this.getActualElapsedTime() - timeZone) != Constants.ZERO) {
			return false;
		}
		// if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
		if (this.getArrDelay() > Constants.ZERO) {
			if (this.getArrDelay() != this.getArrivalDelayNew())
				return false;
		}
		// if ArrDelay < 0 then ArrDelayMinutes should be zero
		if (this.getArrDelay() < Constants.ZERO) {
			if (this.getArrivalDelayNew() != Constants.ZERO)
				return false;
		}
		// if ArrDelayMinutes >= 15 then ArrDel15 should be false
		if (this.getArrivalDelayNew() >= Constants.FIFTEEN) {
			if (this.getArrDel15() == Constants.ZERO)
				return false;
		}
		return dataGood;
	}

	/**
	 * method to check times.
	 * 
	 * @param this
	 *            record read
	 * @return boolean
	 */
	private boolean checkTimes() {
		boolean dataGood = true;

		// getting time difference in minutes
		long duration  = this.getCrsArrTime().getTime()
				- this.getCrsDepTime().getTime();
		long timeDiff = TimeUnit.MILLISECONDS.toMinutes(duration);

		double timeZone = timeDiff - this.getCrsElapsedTime();

		// timeZone % 60 should be 0
		if (timeZone % Constants.NUMOFMINS != Constants.ZERO) {
			return false;
		}
		return dataGood;
	}

	/**
	 * method to check airport ids.
	 * 
	 * @param this
	 *            record read
	 * @return boolean
	 */
	private boolean checkIDData() {
		boolean dataGood = true;
		// AirportID, AirportSeqID, CityMarketID, StateFips, Wac should be
		// larger than 0
		if (this.getDestAirportId() <= Constants.ZERO
				|| this.getOriginAirportId() <= Constants.ZERO
				|| this.getDestAirportSeqId() <= Constants.ZERO
				|| this.getOriginAirportSeqId() <= Constants.ZERO
				|| this.getDestCityMArketId() <= Constants.ZERO
				|| this.getOriginCityMarketId() <= Constants.ZERO
				|| this.getDestStateFips() <= Constants.ZERO
				|| this.getOriginStateFips() <= Constants.ZERO
				|| this.getDestWac() <= Constants.ZERO
				|| this.getOriginWac() <= Constants.ZERO) {
			return false;
		}
		return dataGood;
	}

	/**
	 * method to check origin and destination data.
	 * 
	 * @param this
	 *            record read
	 * @return boolean
	 */
	private boolean checkOrigDestData() {
		boolean dataGood = true;
		// Origin, Destination, CityName, State, StateName should not be
		// empty
		if (this.getOrigin().isEmpty()
				|| this.getDest().isEmpty()
				|| this.getOriginCityName().isEmpty()
				|| this.getDestCityName().isEmpty()
				|| this.getOriginStateNm().isEmpty()
				|| this.getOriginStateAbr().isEmpty()
				|| this.getDestStateNm().isEmpty()
				|| this.getDestStateAbr().isEmpty()) {
			return false;
		}
		return dataGood;
	}
	
	/**
	 * method to convert the record data into a pojo.
	 * 
	 * @param recordData
	 *            record object
	 * @param flightRecord
	 *            record array
	 * @throws BadDataException
	 */
	public void fill(String[] flightRecord, int offset) throws BadDataException{
		try {
			this.setYear(Integer.parseInt(flightRecord[Constants.YEAR+offset]));
			this
					.setMonth(Integer.parseInt(flightRecord[Constants.MONTH+offset]));
			this.setDay(Integer.parseInt(flightRecord[Constants.DAY+offset]));
			this.quarter = Integer.parseInt(flightRecord[Constants.QUARTER+offset]);
			this.setCarrier(flightRecord[Constants.CARRIER+offset]);
			this.fldate = flightRecord[Constants.FDATE+offset];
			this.fnum = flightRecord[Constants.FNUM+offset];
			this.distance = Integer.parseInt(flightRecord[Constants.DIST+offset]);
			this.week = Integer.parseInt(flightRecord[Constants.WEEK+offset]);
			this.arrDelay=Float
					.parseFloat(flightRecord[Constants.ARRDEL+offset]);
			if (flightRecord[Constants.CANCELLED+offset].equals("0")) {
				this.setCancelled(false);
				this.setPrice(Float
						.parseFloat(flightRecord[Constants.AVGPRICE+offset]));
				this.setActualElapsedTime(Integer
						.parseInt(flightRecord[Constants.ACTELAPTIME+offset]));
				this.setArrDel15(Float
						.parseFloat(flightRecord[Constants.ARRDEL15+offset]));
				this.setArrDelay(Float
						.parseFloat(flightRecord[Constants.ARRDEL+offset]));
				this.setArrivalDelayNew(Float
						.parseFloat(flightRecord[Constants.ARRDELNEW+offset]));
			} else {this.setCancelled(true);}
			settingDates(flightRecord,offset);
			this.setCrsElapsedTime(Integer
					.parseInt(flightRecord[Constants.CRSELAPSED+offset]));
			this.setDest(flightRecord[Constants.DEST+offset]);
			this.setDestAirportId(Integer
					.parseInt(flightRecord[Constants.DESTAIRPORTID+offset]));
			this.setDestAirportSeqId(Integer
					.parseInt(flightRecord[Constants.DESTAIRPORTSEQID+offset]));
			this.setDestCityMArketId(Integer
					.parseInt(flightRecord[Constants.DESTMARKETID+offset]));
			this.setDestCityName(flightRecord[Constants.DESTCITYNAME+offset]);
			this.setDestStateAbr(flightRecord[Constants.DESTSTABR+offset]);
			this.setDestStateFips(Integer
					.parseInt(flightRecord[Constants.DESTSTATEFIPS+offset]));
			this.setDestStateNm(flightRecord[Constants.DESTSTATENM+offset]);
			this.setDestWac(Integer
					.parseInt(flightRecord[Constants.DESTWAC+offset]));
			this.setOrigin(flightRecord[Constants.ORIGIN+offset]);
			this.setOriginAirportId(Integer
					.parseInt(flightRecord[Constants.ORIGINAIRPORTID+offset]));
			this.setOriginAirportSeqId(Integer
					.parseInt(flightRecord[Constants.ORIGINAIRPORTSEQID+offset]));
			this.setOriginCityMarketId(Integer
					.parseInt(flightRecord[Constants.ORIGINMARKETID+offset]));
			this
					.setOriginCityName(flightRecord[Constants.ORIGINCITYNAME+offset]);
			this
					.setOriginStateAbr(flightRecord[Constants.ORIGINSTATEABR+offset]);
			this.setOriginStateFips(Integer
					.parseInt(flightRecord[Constants.ORIGINSTATEFIPS+offset]));
			this.setOriginStateNm(flightRecord[Constants.ORIGINSTATENM+offset]);
			this.setOriginWac(Integer
					.parseInt(flightRecord[Constants.ORIGINWAC+offset]));
		} catch (NumberFormatException ne) {
			throw new BadDataException(ne.getMessage());
		}
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
	private void settingDates(String[] flightRecord, int offset) throws BadDataException {
		// setting the scheduled arrival and departure date times
		int cArrTime = Integer
				.parseInt(flightRecord[Constants.CRSARRTIME+offset]);
		int cDepTime = Integer
				.parseInt(flightRecord[Constants.CRSDEPTIME+offset]);
		this.cat = cArrTime;
		this.cdt = cDepTime;
		// exit if these times are zero
		if (cArrTime == 0 || cDepTime == 0) {
			throw new BadDataException("time epmty");
		}
		final Calendar cal = GregorianCalendar.getInstance();
		cal.set(this.getYear(), this.getMonth(),
				this.getDay());
		Date cDepDate;
		Date cArrDate;

		// converting to 00:00 standard time
		if (cArrTime == 2400) {
			cArrTime = 0000;
		}
		// converting to 00:00 standard time
		if (cDepTime == 2400) {
			cDepTime = 0000;
		}
		cal.set(Calendar.HOUR_OF_DAY, (int) cDepTime / 100);
		cal.set(Calendar.MINUTE, (int) cDepTime % 100);
		cDepDate = cal.getTime();

		// incrementing by 1 day if the arrival time is the next day
		if (cArrTime < cDepTime) {
			cal.add(Calendar.DATE, 1);
		}
		cal.set(Calendar.HOUR_OF_DAY, (int) cArrTime / 100);
		cal.set(Calendar.MINUTE, (int) cArrTime % 100);
		cArrDate = cal.getTime();

		// setting the actual arrival and departure date times
		Date depDate = null;
		Date arrDate = null;
		if (!this.isCancelled()) {
			// set calendar to date from record
			cal.set(this.getYear(), this.getMonth(),
					this.getDay());
			int depTime = Integer
					.parseInt(flightRecord[Constants.DEPTIME+offset]);
			// converting to 00:00 standard time
			if (depTime == 2400) {
				depTime = 0000;
			}
			cal.set(Calendar.HOUR_OF_DAY, (int) depTime / 100);
			cal.set(Calendar.MINUTE, (int) depTime % 100);
			depDate = cal.getTime();

			int arrTime = Integer
					.parseInt(flightRecord[Constants.ARRTIME+offset]);
			// converting to 00:00 standard time
			if (arrTime == 2400) {
				arrTime = 0000;
			}
			if (arrTime < depTime) {
				cal.add(Calendar.DATE, 1);
			}
			cal.set(Calendar.HOUR_OF_DAY, (int) arrTime / 100);
			cal.set(Calendar.MINUTE, (int) arrTime % 100);
			arrDate = cal.getTime();

		}

		this.setCrsArrTime(cArrDate);
		this.setCrsDepTime(cDepDate);
		this.setDepTime(depDate);
		this.setArrTime(arrDate);
	}

}
