import org.apache.hadoop.io.WritableComparator;

/**
 * Custom comparator class for secondary sort.
 * @author Abhishek Ravi Chandran
 *
 */
public class CkeyComparator extends WritableComparator {

	public CkeyComparator() {
		super(CustomKey.class, true);
	}

	@Override
	public int compare(Object a, Object b) {
		CustomKey c1 = (CustomKey) a;
		CustomKey c2 = (CustomKey) b;
		if(c1.type != c2.type){
			return c1.type - c2.type;
		} else {
			return -1* (int) (c1.time - c2.time);
		}
	}
}

