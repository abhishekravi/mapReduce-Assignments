package com.cs6200.A7.connections;

import org.apache.hadoop.io.WritableComparator;

/**
 * Custom comparator class for secondary sort(sending incoming flights before outgoing ones).
 * @author Abhishek Ravi Chandran, Mania Abdi
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
		return c1.type - c2.type;
	}
}
