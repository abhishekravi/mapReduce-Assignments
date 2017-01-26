package com.cs6200.A7.modeling;

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
 * Class to pass data between the mappers and reducers for the second phase of
 * program - Data cleaning and Modeling - The object of this class is used as a
 * value
 */
public class ModelingCompositeValue implements Writable {

	public Integer dayOfWeek;
	public String origin;
	public String interm;
	public String dest;
	public Integer layoverHours;
	public Integer totalElapsedHours;
	public Integer totalDistanceGroup;
	public String isMissed;

	public ModelingCompositeValue() {
	}

	public ModelingCompositeValue(Integer dayOfWeek, String origin, String interm, String dest, Integer layoverHours,
			Integer totalElapsedHours, Integer totalDistanceGroup, String isMissed) {
		this.dayOfWeek = dayOfWeek;
		this.origin = origin;
		this.interm = interm;
		this.dest = dest;
		this.layoverHours = layoverHours;
		this.totalElapsedHours = totalElapsedHours;
		this.totalDistanceGroup = totalDistanceGroup;
		this.isMissed = isMissed;
	}

	public void readFields(DataInput in) throws IOException {
		dayOfWeek = in.readInt();
		origin = in.readUTF();
		interm = in.readUTF();
		dest = in.readUTF();
		layoverHours = in.readInt();
		totalElapsedHours = in.readInt();
		totalDistanceGroup = in.readInt();
		isMissed = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(dayOfWeek);
		out.writeUTF(origin);
		out.writeUTF(interm);
		out.writeUTF(dest);
		out.writeInt(layoverHours);
		out.writeInt(totalElapsedHours);
		out.writeInt(totalDistanceGroup);
		out.writeUTF(isMissed);
	}

	@Override
	public String toString() {
		return dayOfWeek + ", " + origin + ", " + interm + ", " + dest + ", " + layoverHours + ", " + totalElapsedHours
				+ ", " + totalDistanceGroup + ", " + isMissed;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dayOfWeek == null) ? 0 : dayOfWeek.hashCode());
		result = prime * result + ((dest == null) ? 0 : dest.hashCode());
		result = prime * result + ((interm == null) ? 0 : interm.hashCode());
		result = prime * result + ((isMissed == null) ? 0 : isMissed.hashCode());
		result = prime * result + ((layoverHours == null) ? 0 : layoverHours.hashCode());
		result = prime * result + ((origin == null) ? 0 : origin.hashCode());
		result = prime * result + ((totalDistanceGroup == null) ? 0 : totalDistanceGroup.hashCode());
		result = prime * result + ((totalElapsedHours == null) ? 0 : totalElapsedHours.hashCode());
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
		ModelingCompositeValue other = (ModelingCompositeValue) obj;
		if (dayOfWeek == null) {
			if (other.dayOfWeek != null)
				return false;
		} else if (!dayOfWeek.equals(other.dayOfWeek))
			return false;
		if (dest == null) {
			if (other.dest != null)
				return false;
		} else if (!dest.equals(other.dest))
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
		if (origin == null) {
			if (other.origin != null)
				return false;
		} else if (!origin.equals(other.origin))
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
		return true;
	}
}
