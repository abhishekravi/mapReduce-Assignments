package com.cs6200.A7.connections;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.cs6200.A7.data.Flight;
import com.cs6200.A7.data.RecordData;
import com.google.common.collect.ComparisonChain;

/**
 * Custom writable class.
 * 
 * @author Abhishek Ravi Chandran
 * @author Chinmayee Vaidhya
 * @author Mania Abdi
 *
 */
public class CWritable implements WritableComparable<CWritable> {

	int day;
	int month;
	int dayofweek;
	String origin;
	String dest;
	int carrtime;
	int cdeptime;
	int elapsed;
	int dist;
	int fnum;
	
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
		
		this.day = a.day;
		this.month = a.month;
		this.dayofweek = a.dayofweek;
		this.origin = a.origin;
		this.dest = a.dest;
		this.carrtime = a.carrtime;
		this.cdeptime = a.cdeptime;
		this.elapsed = a.elapsed;
		this.dist = a.dist;
		this.fnum = a.fnum;
		
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
		this.day = a.day;
		this.month = a.month;
		this.dayofweek = a.dayofweek;
		this.origin = a.origin;
		this.dest = a.dest;
		this.carrtime = a.cat;
		this.cdeptime = a.cdt;
		this.elapsed = a.crsElapsedTime;
		this.dist = a.distgrp;
		
		
		
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
		this.fnum = a.fnum;
	}
	
	/**
	 * constructor to create object form RecordData
	 * @param a
	 * Flight
	 * @param type
	 * type of record
	 */
	public CWritable(Flight a, int type) {
		this.day = a.day;
		this.month = a.month;
		this.dayofweek = a.dayofweek;
		this.origin = a.origin;
		this.dest = a.dest;
		this.carrtime = a.cat;
		this.cdeptime = a.cdt;
		this.elapsed = a.crsElapsedTime;
		this.dist = a.distgrp;
		
		
		
		if(type == 0){
			this.stime = a.crsArrTime.getTime();
			this.atime = 0;
			this.city = a.dest;
		}
		else {
			this.stime = a.crsDepTime.getTime();
			this.atime = 0;
			this.city = a.origin;
		}
		this.year = a.year;
		this.type = type;
		this.fnum = a.fnum;
	}
	
	

	@Override
	public void readFields(DataInput in) throws IOException {
		this.day = in.readInt();
		this.month = in.readInt();
		this.dayofweek = in.readInt();
		this.origin = in.readUTF();
		this.dest = in.readUTF();
		this.carrtime = in.readInt();
		this.cdeptime = in.readInt();
		this.elapsed = in.readInt();
		this.dist = in.readInt();
		this.fnum = in.readInt();
		
		
		this.stime = in.readLong();
		this.atime = in.readLong();
		this.cancelled = in.readInt();
		this.year = in.readInt();
		this.type = in.readInt();
		this.city = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.day);
		out.writeInt(this.month);
		out.writeInt(this.dayofweek);
		out.writeUTF(this.origin);
		out.writeUTF(this.dest);
		out.writeInt(this.carrtime);
		out.writeInt(this.cdeptime);
		out.writeInt(this.elapsed);
		out.writeInt(this.dist);
		out.writeInt(this.fnum);
		
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
				.compare(this.day, o.day)
				.compare(this.month, o.month)
				.compare(this.dayofweek, o.dayofweek)
				.compare(this.origin, o.origin)
				.compare(this.dest, o.dest)
				.compare(this.carrtime, o.carrtime)
				.compare(this.cdeptime, o.cdeptime)
				.compare(this.elapsed, o.elapsed)
				.compare(this.dist, o.dist)
				.compare(this.fnum, o.fnum)
				.compare(this.stime, o.stime)
				.compare(this.atime, o.atime)
				.compare(this.cancelled, o.cancelled)
				.compare(this.year, o.year)
				.compare(this.type, o.type)
				.compare(this.city, o.city).result();
	}
}