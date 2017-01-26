/**
 * class to hold data from test file.
 * @author Abhishek Ravi Chandran
 *
 */
public class Flight {

	int year;
	int month;
	int week;
	int day;
	int quarter;
	int distance;
	String fnum;
	String fldate;
	boolean cancelled;
	float arrDel;
	int cat;
	int cdt;
	int crsElapsedTime;
	String origin;
	String dest;
	String carrier;
	float arrDelay;
	
	/**
	 * method to set data.
	 * @param flightRecord
	 * string array of data
	 * @param offset
	 * offset of index
	 * @throws BadDataException
	 */
	public void fill(String[] flightRecord, int offset) throws BadDataException{
		try {
			this.year = Integer.parseInt(flightRecord[Constants.YEAR+offset]);
			this.month = Integer.parseInt(flightRecord[Constants.MONTH+offset]);
			this.day = (Integer.parseInt(flightRecord[Constants.DAY+offset]));
			this.quarter = Integer.parseInt(flightRecord[Constants.QUARTER+offset]);
			this.week = Integer.parseInt(flightRecord[Constants.WEEK+offset]);
			this.distance = Integer.parseInt(flightRecord[Constants.DIST+offset]);
			this.carrier = (flightRecord[Constants.CARRIER+offset]);
			this.fldate = flightRecord[Constants.FDATE+offset];
			this.fnum = flightRecord[Constants.FNUM+offset];
			this.crsElapsedTime = (Integer
					.parseInt(flightRecord[Constants.CRSELAPSED+offset]));
			this.cat = Integer
					.parseInt(flightRecord[Constants.CRSARRTIME+offset]);
			this.cdt = Integer
					.parseInt(flightRecord[Constants.CRSDEPTIME+offset]);
			this.dest = (flightRecord[Constants.DEST+offset]);
			this.origin = (flightRecord[Constants.ORIGIN+offset]);
		} catch (NumberFormatException ne) {
			throw new BadDataException(ne.getMessage());
		}
	}

}
