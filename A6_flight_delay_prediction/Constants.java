import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class to hold constants used in the program.
 * 
 * @author Abhishek Ravi Chandran
 * @author Chinmayee Vaidhya
 *
 */
public final class Constants {

	// position of values in input records
	// --------------records data starts----------------
	static final int YEAR = 0;
	static final int QUARTER = 1;
	static final int DIST = 54;
	static final int MONTH = 2;
	static final int WEEK = 4;
	static final int DAY = 3;
	static final int FDATE = 5;
	static final int FNUM = 10;
	static final int CARRIER = 8;
	static final int CANCELLED = 47;
	static final int AVGPRICE = 109;
	static final int ACTELAPTIME = 51;
	static final int ARRDEL15 = 44;
	static final int ARRDEL = 42;
	static final int ARRDELNEW = 43;
	static final int CRSELAPSED = 50;
	static final int DEST = 23;
	static final int DESTAIRPORTID = 20;
	static final int DESTAIRPORTSEQID = 21;
	static final int DESTMARKETID = 22;
	static final int DESTCITYNAME = 24;
	static final int DESTSTABR = 25;
	static final int DESTSTATEFIPS = 26;
	static final int DESTSTATENM = 27;
	static final int DESTWAC = 28;
	static final int ORIGIN = 14;
	static final int ORIGINAIRPORTID = 11;
	static final int ORIGINAIRPORTSEQID = 12;
	static final int ORIGINMARKETID = 13;
	static final int ORIGINCITYNAME = 15;
	static final int ORIGINSTATEABR = 16;
	static final int ORIGINSTATEFIPS = 17;
	static final int ORIGINSTATENM = 18;
	static final int ORIGINWAC = 19;
	static final int CRSARRTIME = 40;
	static final int CRSDEPTIME = 29;
	static final int DEPTIME = 30;
	static final int ARRTIME = 41;
	// --------------records data ends----------------
	static final int ZERO = 0;
	static final int ARG1 = 0;
	static final int ARG2 = 1;
	static final int ARG3 = 2;
	static final int ARG4 = 3;
	static final int ARG5 = 4;
	static final int ARG6 = 5;
	static final int ARG7 = 6;
	static final int FIFTEEN = 15;
	static final int NUMOFMINS = 60;
	// map reduce job number of arguments
	static final int MRARGSIZE = 7;
	// plain java job number of arguments
	static final int PJARGSIZE = 2;
	static final int HOLIDAYGAP = 2;
	static final Set<String> CITIES = new HashSet<String>();

	static final List<Calendar> HOLIDAYS = new ArrayList<Calendar>();
	static {
		// holidays 1995
		HOLIDAYS.add(new GregorianCalendar(1995, 11, 23));
		HOLIDAYS.add(new GregorianCalendar(1995, 12, 25));
		HOLIDAYS.add(new GregorianCalendar(1995, 1, 1));
		HOLIDAYS.add(new GregorianCalendar(1995, 12, 31));
		HOLIDAYS.add(new GregorianCalendar(1995, 10, 31));
		HOLIDAYS.add(new GregorianCalendar(1995, 7, 4));

		// holidays 1996
		HOLIDAYS.add(new GregorianCalendar(1996, 12, 25));
		HOLIDAYS.add(new GregorianCalendar(1996, 11, 28));
		HOLIDAYS.add(new GregorianCalendar(1996, 1, 1));
		HOLIDAYS.add(new GregorianCalendar(1996, 12, 31));
		HOLIDAYS.add(new GregorianCalendar(1996, 10, 31));
		HOLIDAYS.add(new GregorianCalendar(1996, 7, 4));

		// holidays 1997
		HOLIDAYS.add(new GregorianCalendar(1997, 12, 25));
		HOLIDAYS.add(new GregorianCalendar(1997, 11, 27));
		HOLIDAYS.add(new GregorianCalendar(1997, 1, 1));
		HOLIDAYS.add(new GregorianCalendar(1997, 12, 31));
		HOLIDAYS.add(new GregorianCalendar(1997, 10, 31));
		HOLIDAYS.add(new GregorianCalendar(1997, 7, 4));

		// holidays 1998
		HOLIDAYS.add(new GregorianCalendar(1998, 12, 25));
		HOLIDAYS.add(new GregorianCalendar(1998, 11, 26));
		HOLIDAYS.add(new GregorianCalendar(1998, 1, 1));
		HOLIDAYS.add(new GregorianCalendar(1998, 12, 31));
		HOLIDAYS.add(new GregorianCalendar(1998, 10, 31));
		HOLIDAYS.add(new GregorianCalendar(1998, 7, 4));

		// popular cities
		CITIES.add("ATL");
		CITIES.add("ORD");
		CITIES.add("DFW");
		CITIES.add("LAX");
		CITIES.add("DEN");
		CITIES.add("IAH");
		CITIES.add("PHX");
		CITIES.add("SFO");
		CITIES.add("CLT");
		CITIES.add("DTW");
		CITIES.add("MSP");
		CITIES.add("LAS");
		CITIES.add("MCO");
		CITIES.add("EWR");
		CITIES.add("JFK");
		CITIES.add("LGA");
		CITIES.add("BOS");
		CITIES.add("SLC");
		CITIES.add("SEA");
		CITIES.add("BWI");
		CITIES.add("MIA");
		CITIES.add("MDW");
		CITIES.add("PHL");
		CITIES.add("SAN");
		CITIES.add("FLL");
		CITIES.add("TPA");
		CITIES.add("DCA");
		CITIES.add("IAD");
		CITIES.add("HOU");
	}

	static final String ATTRIBUTES = "@attribute carrier {AA,AS,CO,DL,HP,NW,TW,UA,US,WN}\n"
			+ "@attribute quarter {1,2,3,4}\n"
			+ "@attribute month {1,2,3,4,5,6,7,8,9,10,11,12}\n"
			+ "@attribute week {1,2,3,4,5,6,7}\n"
			+ "@attribute at {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24}\n"
			+ "@attribute dt {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24}\n"
			+ "@attribute et {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53}\n"
			+ "@attribute distance {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53}\n"
			+ "@attribute origin {ATL,ORD,DFW,LAX,DEN,IAH,PHX,SFO,CLT,DTW,MSP,LAS,MCO,EWR,JFK,LGA,BOS,SLC,SEA,BWI,MIA,MDW,PHL,SAN,FLL,TPA,DCA,IAD,HOU,??}\n"
			+ "@attribute dest {ATL,ORD,DFW,LAX,DEN,IAH,PHX,SFO,CLT,DTW,MSP,LAS,MCO,EWR,JFK,LGA,BOS,SLC,SEA,BWI,MIA,MDW,PHL,SAN,FLL,TPA,DCA,IAD,HOU,??}\n"
			+ "@attribute holiday {0,1}\n" + "@attribute late {TRUE,FALSE}\n";
}
