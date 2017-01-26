package com.cs6200.A7.data;

/**
 * App interface that every stand alone application
 * should implement for the Utils class to run 
 * the application using it's runApp method
 * 
 *  @author Chintan Pathak
 */
public interface App {

	/**
	 * The application logic that is to be 
	 * executed by the stand alone application
	 *  
	 * @param objects
	 */
	public abstract void runApp(Object... objects);
}
