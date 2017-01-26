import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

/**
 * custom writable class.
 * 
 * @author Abhishek Ravi Chandran
 * @author Chinmayee Vaidhya
 *
 */
public class CWritable implements WritableComparable<CWritable> {

	long stime;
	long atime;
	int cancelled;
	int year;
	int type;
	String city;


	/**
	 * Default constructor.
	 */
	public CWritable() {

	}
	
	/**
	 * Constructor to create a copy of the object.
	 * @param a
	 * CWritable
	 */
	public CWritable(CWritable a) {
		this.stime = a.stime;
		this.atime = a.atime;
		this.cancelled = a.cancelled;
		this.year = a.year;
		this.type = a.type;
		this.city = a.city; 
	}
	
	/**
	 * constructor to create object form RecordData
	 * @param a
	 * RecordData
	 * @param type
	 * type of record
	 */
	public CWritable(RecordData a, int type) {
		if(type == 0){
			this.stime = a.crsArrTime.getTime();
			this.atime = a.arrTime.getTime();
			this.city = a.dest;
		}
		else {
			this.stime = a.crsDepTime.getTime();
			this.atime = a.depTime.getTime();
			this.city = a.origin;
		}
		this.cancelled = a.isCancelled() ? 1:0;
		this.year = a.year;
		this.type = type;
	}
	
	

	@Override
	public void readFields(DataInput in) throws IOException {
		this.stime = in.readLong();
		this.atime = in.readLong();
		this.cancelled = in.readInt();
		this.year = in.readInt();
		this.type = in.readInt();
		this.city = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.stime);
		out.writeLong(this.atime);
		out.writeInt(this.cancelled);
		out.writeInt(this.year);
		out.writeInt(this.type);
		out.writeUTF(this.city);
	}

	@Override
	public int compareTo(CWritable o) {
		return ComparisonChain.start()
				.compare(this.stime, o.stime)
				.compare(this.atime, o.atime)
				.compare(this.cancelled, o.cancelled)
				.compare(this.year, o.year)
				.compare(this.type, o.type)
				.compare(this.city, o.city).result();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (atime ^ (atime >>> 32));
		result = prime * result + cancelled;
		result = prime * result + ((city == null) ? 0 : city.hashCode());
		result = prime * result + (int) (stime ^ (stime >>> 32));
		result = prime * result + type;
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
		CWritable other = (CWritable) obj;
		if (atime != other.atime)
			return false;
		if (cancelled != other.cancelled)
			return false;
		if (city == null) {
			if (other.city != null)
				return false;
		} else if (!city.equals(other.city))
			return false;
		if (stime != other.stime)
			return false;
		if (type != other.type)
			return false;
		if (year != other.year)
			return false;
		return true;
	}

	
	
}
