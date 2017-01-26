package com.cs6200.A7.connections;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.cs6200.A7.data.Flight;
import com.cs6200.A7.data.RecordData;
import com.google.common.collect.ComparisonChain;

/**
 * Custom key class.
 * @author Abhishek Ravi Chandran, Mania Abdi
 *
 */
public class CustomKey implements WritableComparable<CustomKey> {

	String cyear;
	int type;

	public CustomKey() {
		super();
	}

	/**
	 * constructor with record data and type
	 * @param r
	 * record data
	 * @param type
	 * incoming or outgoing
	 * @param rtype
	 * history or test
	 */
	public CustomKey(RecordData r, int type, int rtype) {
		this.cyear = r.carrier + "," + r.year + "," + rtype;
		this.type = type;
	}
	
	/**
	 * constructor with record data and type
	 * @param r
	 * Flight data
	 * @param type
	 * incoming or outgoing
	 * @param rtype
	 * history or test
	 */
	public CustomKey(Flight r, int type, int rtype) {
		this.cyear = r.carrier + "," + r.year + "," + rtype;
		this.type = type;
	}

	@Override
	public void readFields(DataInput di) throws IOException {
		this.cyear = di.readUTF();
		this.type = di.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.cyear);
		out.writeInt(this.type);
	}

	@Override
	public int compareTo(CustomKey o) {
		return ComparisonChain.start().compare(this.cyear, o.cyear)
				.compare(this.type, o.type).result();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cyear == null) ? 0 : cyear.hashCode());
		result = prime * result + type;
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
		CustomKey other = (CustomKey) obj;
		if (cyear == null) {
			if (other.cyear != null)
				return false;
		} else if (!cyear.equals(other.cyear))
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	

}
