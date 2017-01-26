package com.cs6200.A7.data;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import com.cs6200.A7.exception.BadDataException;

/**
 * Class to hold test data.
 * 
 * @author Abhishek Ravi Chandran
 *
 */
public class Flight {

	public int year;
	public int month;
	public int week;
	public int day;
	public int dayofweek;
	public int quarter;
	public int distance;
	public int distgrp;
	public int fnum;
	public String fldate;
	public boolean cancelled;
	public int cat;
	public int cdt;
	public Date crsArrTime;
	public Date crsDepTime;
	public int crsElapsedTime;
	public String origin;
	public String dest;
	public String carrier;

	/**
	 * method to convert the record data into a pojo.
	 * 
	 * @param recordData
	 *            record object
	 * @param flightRecord
	 *            record array
	 * @throws BadDataException
	 */
	public void fill(String[] flightRecord, int offset) throws BadDataException {
		try {
			this.year = Integer.parseInt(flightRecord[Constants.YEAR + offset]);
			this.month = Integer.parseInt(flightRecord[Constants.MONTH + offset]);
			this.day = (Integer.parseInt(flightRecord[Constants.DAY + offset]));
			this.distgrp = Integer.parseInt(flightRecord[Constants.DISTGRP + offset]);
			this.dayofweek = Integer.parseInt(flightRecord[Constants.WEEK + offset]);
			this.quarter = Integer.parseInt(flightRecord[Constants.QUARTER + offset]);
			this.week = Integer.parseInt(flightRecord[Constants.WEEK + offset]);
			this.distance = Integer.parseInt(flightRecord[Constants.DIST + offset]);
			this.carrier = (flightRecord[Constants.CARRIER + offset]);
			this.fldate = flightRecord[Constants.FDATE + offset];
			this.fnum = Integer.parseInt(flightRecord[Constants.FNUM + offset]);
			settingDates(flightRecord, offset);
			this.crsElapsedTime = (Integer.parseInt(flightRecord[Constants.CRSELAPSED + offset]));
			this.cat = Integer.parseInt(flightRecord[Constants.CRSARRTIME + offset]);
			this.cdt = Integer.parseInt(flightRecord[Constants.CRSDEPTIME + offset]);
			this.dest = (flightRecord[Constants.DEST + offset]);
			this.origin = (flightRecord[Constants.ORIGIN + offset]);
		} catch (NumberFormatException ne) {
			throw new BadDataException(ne.getMessage());
		}
	}

	/**
	 * method to set date values.
	 * 
	 * @param flightRecord
	 *            record values
	 * @param offset
	 *            offset
	 */
	private void settingDates(String[] flightRecord, int offset) {
		int cArrTime = Integer.parseInt(flightRecord[Constants.CRSARRTIME + offset]);
		int cDepTime = Integer.parseInt(flightRecord[Constants.CRSDEPTIME + offset]);
		this.cat = cArrTime;
		this.cdt = cDepTime;
		final Calendar cal = GregorianCalendar.getInstance();
		cal.set(this.year, this.month, this.day);
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

		this.crsArrTime = cArrDate;
		this.crsDepTime = cDepDate;
	}

}
