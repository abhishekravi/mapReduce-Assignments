import java.time.LocalDateTime;

/**
 * 
 * @author Abhishek Ravi Chandran
 * This class will hold a record data.
 *
 */
public class RecordData {

	private int year;
	private int month;
	private boolean cancelled;
	private double avgTicketPrice;
	private LocalDateTime arrTime;
	private LocalDateTime depTime;
	private double arrDelay;
	private int actualElapsedTime;
	private double arrivalDelayNew;
	private double arrDel15;
	private LocalDateTime crsArrTime;
	private LocalDateTime crsDepTime;
	private int crsElapsedTime;
	private int destAirportId;
	private int originAirportId;
	private int destAirportSeqId;
	private int originAirportSeqId;
	private int destCityMArketId;
	private int originCityMarketId;
	private int destStateFips;
	private int OriginStateFips;
	private int DestWac;
	private int OriginWac;
	private String origin;
	private String dest;
	private String originCityName;
	private String destCityName;
	private String originStateNm;
	private String originStateAbr;
	private String destStateNm;
	private String destStateAbr;
	private String carrier;

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

	public double getAvgTicketPrice() {
		return avgTicketPrice;
	}

	public void setAvgTicketPrice(double avgTicketPrice) {
		this.avgTicketPrice = avgTicketPrice;
	}

	public LocalDateTime getArrTime() {
		return arrTime;
	}

	public void setArrTime(LocalDateTime arrTime) {
		this.arrTime = arrTime;
	}

	public LocalDateTime getDepTime() {
		return depTime;
	}

	public void setDepTime(LocalDateTime depTime) {
		this.depTime = depTime;
	}

	public double getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(double arrDelay) {
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

	public void setArrivalDelayNew(double arrivalDelayNew) {
		this.arrivalDelayNew = arrivalDelayNew;
	}

	public double getArrDel15() {
		return arrDel15;
	}

	public void setArrDel15(double arrDel15) {
		this.arrDel15 = arrDel15;
	}

	public LocalDateTime getCrsArrTime() {
		return crsArrTime;
	}

	public void setCrsArrTime(LocalDateTime crsArrTime) {
		this.crsArrTime = crsArrTime;
	}

	public LocalDateTime getCrsDepTime() {
		return crsDepTime;
	}

	public void setCrsDepTime(LocalDateTime crsDepTime) {
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

}
