package com.cs6200.A7.prediction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * @author Chintan Pathak, Chinmayee Vaidya
 *
 */

/**
 * Class to pass data between the mappers and reducers for the third phase of
 * program - Prediction 
 * - The object of this class is used as a value
 */
public class PredictionCompositeValue implements Writable {

	public Integer dayOfWeek;
	public String interm;
	public Integer layoverHours;
	public Integer totalElapsedHours;
	public Integer totalDistanceGroup;
	public Boolean isMissed;
	public Integer firstFlightNo;
	public Integer secondFlightNo;
	public Integer totalElapsedTime;
	public String uniqueCarrier;
	public int month;
	public int day;
	public int year;
	public int type;

	public PredictionCompositeValue() {
	}

	public PredictionCompositeValue(Integer dayOfWeek, String interm, Integer layoverHours, Integer totalElapsedHours,
			Integer totalDistanceGroup, Boolean isMissed, Integer firstFlightNo, Integer secondFlightNo,
			Integer totalElapsedTime, String uniqueCarrier, Integer month, Integer day, Integer year, int type) {
		this.dayOfWeek = dayOfWeek;
		this.interm = interm;
		this.layoverHours = layoverHours;
		this.totalDistanceGroup = totalDistanceGroup;
		this.isMissed = isMissed;
		this.firstFlightNo = firstFlightNo;
		this.secondFlightNo = secondFlightNo;
		this.totalElapsedTime = totalElapsedTime;
		this.totalElapsedHours = (totalElapsedTime / 60);
		this.uniqueCarrier = uniqueCarrier;
		this.month = month;
		this.day = day;
		this.year = year;
		this.type = type;
	}

	public PredictionCompositeValue(PredictionCompositeValue o) {
		this.dayOfWeek = o.dayOfWeek;
		this.interm = o.interm;
		this.layoverHours = o.layoverHours;
		this.totalDistanceGroup = o.totalDistanceGroup;
		this.isMissed = o.isMissed;
		this.firstFlightNo = o.firstFlightNo;
		this.secondFlightNo = o.secondFlightNo;
		this.totalElapsedTime = o.totalElapsedTime;
		this.totalElapsedHours = o.totalElapsedHours;
		this.uniqueCarrier = o.uniqueCarrier;
		this.month = o.month;
		this.day = o.day;
		this.year = o.year;
		this.type = o.type;
	}

	public void readFields(DataInput in) throws IOException {
		dayOfWeek = in.readInt();
		interm = in.readUTF();
		layoverHours = in.readInt();
		totalDistanceGroup = in.readInt();
		isMissed = in.readBoolean();
		totalElapsedHours = in.readInt();
		firstFlightNo = in.readInt();
		secondFlightNo = in.readInt();
		totalElapsedTime = in.readInt();
		uniqueCarrier = in.readUTF();
		month = in.readInt();
		day = in.readInt();
		year = in.readInt();
		type = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(dayOfWeek);
		out.writeUTF(interm);
		out.writeInt(layoverHours);
		out.writeInt(totalElapsedHours);
		out.writeInt(totalDistanceGroup);
		out.writeBoolean(isMissed);
		out.writeInt(firstFlightNo);
		out.writeInt(secondFlightNo);
		out.writeInt(totalElapsedTime);
		out.writeUTF(uniqueCarrier);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(year);
		out.writeInt(type);
	}

	@Override
	public String toString() {
		return dayOfWeek + ", " + interm + ", " + layoverHours + ", " + totalElapsedHours + ", " + totalDistanceGroup
				+ ", " + isMissed + ", " + firstFlightNo + ", " + secondFlightNo + ", " + totalElapsedTime + ", "
				+ uniqueCarrier + ", " + month + ", " + day + ", " + year + ", " + type;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + day;
		result = prime * result + ((dayOfWeek == null) ? 0 : dayOfWeek.hashCode());
		result = prime * result + ((firstFlightNo == null) ? 0 : firstFlightNo.hashCode());
		result = prime * result + ((interm == null) ? 0 : interm.hashCode());
		result = prime * result + ((isMissed == null) ? 0 : isMissed.hashCode());
		result = prime * result + ((layoverHours == null) ? 0 : layoverHours.hashCode());
		result = prime * result + month;
		result = prime * result + ((secondFlightNo == null) ? 0 : secondFlightNo.hashCode());
		result = prime * result + ((totalDistanceGroup == null) ? 0 : totalDistanceGroup.hashCode());
		result = prime * result + ((totalElapsedHours == null) ? 0 : totalElapsedHours.hashCode());
		result = prime * result + ((totalElapsedTime == null) ? 0 : totalElapsedTime.hashCode());
		result = prime * result + type;
		result = prime * result + ((uniqueCarrier == null) ? 0 : uniqueCarrier.hashCode());
		result = prime * result + year;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PredictionCompositeValue other = (PredictionCompositeValue) obj;
		if (day != other.day)
			return false;
		if (dayOfWeek == null) {
			if (other.dayOfWeek != null)
				return false;
		} else if (!dayOfWeek.equals(other.dayOfWeek))
			return false;
		if (firstFlightNo == null) {
			if (other.firstFlightNo != null)
				return false;
		} else if (!firstFlightNo.equals(other.firstFlightNo))
			return false;
		if (interm == null) {
			if (other.interm != null)
				return false;
		} else if (!interm.equals(other.interm))
			return false;
		if (isMissed == null) {
			if (other.isMissed != null)
				return false;
		} else if (!isMissed.equals(other.isMissed))
			return false;
		if (layoverHours == null) {
			if (other.layoverHours != null)
				return false;
		} else if (!layoverHours.equals(other.layoverHours))
			return false;
		if (month != other.month)
			return false;
		if (secondFlightNo == null) {
			if (other.secondFlightNo != null)
				return false;
		} else if (!secondFlightNo.equals(other.secondFlightNo))
			return false;
		if (totalDistanceGroup == null) {
			if (other.totalDistanceGroup != null)
				return false;
		} else if (!totalDistanceGroup.equals(other.totalDistanceGroup))
			return false;
		if (totalElapsedHours == null) {
			if (other.totalElapsedHours != null)
				return false;
		} else if (!totalElapsedHours.equals(other.totalElapsedHours))
			return false;
		if (totalElapsedTime == null) {
			if (other.totalElapsedTime != null)
				return false;
		} else if (!totalElapsedTime.equals(other.totalElapsedTime))
			return false;
		if (type != other.type)
			return false;
		if (uniqueCarrier == null) {
			if (other.uniqueCarrier != null)
				return false;
		} else if (!uniqueCarrier.equals(other.uniqueCarrier))
			return false;
		if (year != other.year)
			return false;
		return true;
	}
}
