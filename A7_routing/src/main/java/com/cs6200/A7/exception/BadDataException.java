package com.cs6200.A7.exception;

/**
 * custom exception class for bad data.
 * 
 * @author Abhishek Ravi Chandran
 *
 */
public class BadDataException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * constructor to pass error message.
	 * 
	 * @param msg
	 *            error message
	 */
	public BadDataException(String msg) {
		super(msg);
	}
}
