import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

/**
 * custom writable class for composite key of carrier and month.
 * 
 * @author Abhishek Ravi Chandran
 * @author Chinmayee Vaidya
 *
 */
public class CarrierMonthWritable implements
		WritableComparable<CarrierMonthWritable> {

	private String carrier;
	private int month;

	/**
	 * Default constructor.
	 */
	public CarrierMonthWritable() {

	}

	/**
	 * parameterized constructor.
	 * @param month
	 * month data
	 * @param carrier
	 * carrier name
	 */
	public CarrierMonthWritable(int month, String carrier) {
		this.month = month;
		this.carrier = carrier;
	}

	@Override
	public String toString() {

		return (new StringBuilder()).append(month).append(' ').append(carrier)
				.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		month = in.readInt();
		carrier = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(month);
		out.writeUTF(carrier);
	}

	@Override
	public int compareTo(CarrierMonthWritable o) {
		return ComparisonChain.start().compare(month, o.month)
				.compare(carrier, o.carrier).result();
	}

}
