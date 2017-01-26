import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

/**
 * Custom key class.
 * @author Abhishek Ravi Chandran
 *
 */
public class CustomKey implements WritableComparable<CustomKey> {

	String cyear;
	int type;
	long time;
	String city;

	public CustomKey() {
		super();
	}

	/**
	 * constructor with record data and type
	 * @param r
	 * record data
	 * @param type
	 * type of record
	 */
	public CustomKey(RecordData r, int type) {
		this.cyear = r.carrier + "," + r.year;
		this.type = type;
		if (type == 0) {
			this.time = r.crsArrTime.getTime();
			this.city = r.dest;
		} else {
			this.time = r.crsDepTime.getTime();
			this.city = r.origin;
		}

	}

	@Override
	public void readFields(DataInput di) throws IOException {
		this.cyear = di.readUTF();
		this.type = di.readInt();
		this.time = di.readLong();
		this.city = di.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.cyear);
		out.writeInt(this.type);
		out.writeLong(this.time);
		out.writeUTF(this.city);
	}

	@Override
	public int compareTo(CustomKey o) {
		return ComparisonChain.start().compare(this.cyear, o.cyear)
				.compare(this.type, o.type)
				.compare(this.time, o.time)
				.compare(this.city, o.city).result();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((city == null) ? 0 : city.hashCode());
		result = prime * result + ((cyear == null) ? 0 : cyear.hashCode());
		result = prime * result + (int) (time ^ (time >>> 32));
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
		if (city == null) {
			if (other.city != null)
				return false;
		} else if (!city.equals(other.city))
			return false;
		if (cyear == null) {
			if (other.cyear != null)
				return false;
		} else if (!cyear.equals(other.cyear))
			return false;
		if (time != other.time)
			return false;
		if (type != other.type)
			return false;
		return true;
	}
	

}

