package com.cs6270.A8;

import scala.Serializable;

/**
 * Class to hold data that is needed.
 * @author Abhishek Ravi Chandran
 *
 */
public class FData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 130099573270330961L;
	
	/**
	 * Constructor that converts RecordData to FData.
	 * @param r
	 * RecordData
	 */
	FData(RecordData r){
		this.year = r.year;
		this.carrier = r.carrier;
		this.etime = r.crsElapsedTime;
		this.price = r.price;
		this.fdate = r.fldate;
		
	}
	
	int year;
	String carrier;
	int etime;
	float price;
	String fdate;
}
