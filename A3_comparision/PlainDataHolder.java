import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * common data holder for plain java solutions.
 * 
 * @author Abhishek Ravi Chandran
 *
 */
public class PlainDataHolder {

	/**
	 * class to hold the airline data.
	 * 
	 * @author Abhishek Ravi Chandran
	 *
	 */
	class Data {
		Data() {
			this.records = new HashMap<String, Carrier>();
		}

		// contain all the records in a map with the carrier as the key
		Map<String, Carrier> records;
		// holding all the headers
	}

	/**
	 * class to hold data for individual carriers
	 * 
	 * @author Abhishek Ravi Chandran
	 *
	 */
	class Carrier {

		Carrier() {
			prices = new ArrayList<Float>();
		}

		List<Float> prices;
		// name of the carrier
		String carrier;
		// number of flights
		int count;
		// total price
		double totalPrice;
		// mean price for this carrier
		double statVal;
		// month data
		int month;
	}

	/**
	 * factory method to get new Carrier object.
	 * 
	 * @return Carrier object
	 */
	public Carrier getNewCarrier() {
		return new Carrier();
	}

	/**
	 * factory method to get new Data object.
	 * 
	 * @return data object
	 */
	public Data getNewData() {
		return new Data();
	}

	/**
	 * method to calculate the average or median.
	 * 
	 * @param data
	 *            flight data
	 */
	public static void calculateStat(Data data, String mode) {
		switch (mode) {
		case "-mn":
			// getting mean
			for (Entry<String, Carrier> k : data.records.entrySet()) {
				System.out.println(k.getKey()
						+ " "
						+ Util.round(k.getValue().totalPrice
								/ k.getValue().count, 2));
			}
			break;
		case "-md":
			// getting median
			for (Entry<String, Carrier> k : data.records.entrySet()) {
				System.out.println(k.getKey()
						+ " "
						+ Util.round(Util.calculateMedian(k.getValue().prices),
								2));
			}
			break;
		case "-fm":
			// getting fast median using quick select
			for (Entry<String, Carrier> k : data.records.entrySet()) {
				System.out.println(k.getKey() + " "
						+ Util.round(Util.fastMedian(k.getValue().prices), 2));
			}
			break;
		}
	}
}
