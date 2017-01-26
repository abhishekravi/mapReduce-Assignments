/**
 * custom exception class for bad data.
 * @author Abhishek Ravi Chandran
 *
 */
public class BadDataException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	BadDataException(String msg){
		super(msg);
	}
}
