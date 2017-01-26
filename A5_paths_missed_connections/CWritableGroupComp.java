import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * to group with natural key, carrier and year.
 * @author Abhishek Ravi Chandran
 *
 */
public class CWritableGroupComp extends WritableComparator {
	
	public CWritableGroupComp() {
		super(CustomKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CustomKey c1 = (CustomKey) a;
		CustomKey c2 = (CustomKey) b;
		return c1.cyear.compareTo(c2.cyear);
	}
}
