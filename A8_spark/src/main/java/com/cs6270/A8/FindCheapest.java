package com.cs6270.A8;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Class to calculate the cheapest carrier.
 * @author Abhishek Ravi Chandran
 *
 */
public class FindCheapest {

	/**
	 * method to find cheapest carrier.
	 * @param reg
	 * map that has all the values calculated through linear regression
	 * @return
	 * cheapest carrier
	 */
	public String findCheapest(Map<String, Float> reg){
		Map<String, Integer> carrCount = new HashMap<String, Integer>();
		Map<String,YearData> yearDatas = new HashMap<String,YearData>();
		YearData year;
		Carrier car;
		for(Entry<String, Float> e : reg.entrySet()) {
				String keys[] = e.getKey().split("\\s+");
				carrCount.put(keys[0], 0);
				if(yearDatas.get(keys[1]) == null){
					yearDatas.put(keys[1],new YearData());
				}
				year = yearDatas.get(keys[1]);
				car = new Carrier();
				car.name = keys[0];
				car.price = e.getValue();
				year.vals.add(car);
			}
		
		for(Entry<String, YearData> e: yearDatas.entrySet()){
			float min = Float.MAX_VALUE;
			String ca = "";
			for(Carrier c : e.getValue().vals){
				if(c.price<min){
					min = c.price;
					ca = c.name;
				}
			}
			carrCount.put(ca,carrCount.get(ca) + 1);
		}
		return getCheapest(carrCount);
	}

	/**
	 * method to get cheapest after comparisons.
	 * @param carrCount
	 * map having counts
	 * @return
	 * cheapest carrier
	 */
	private String getCheapest(Map<String, Integer> carrCount) {
		String carr = "";
		int max = 0;
		for (Entry<String, Integer> e : carrCount.entrySet()) {
			if (e.getValue() > max) {
				carr = e.getKey();
				max = e.getValue();
			}
		}
		return carr;
	}

	/**
	 * Class to hold values for a year.
	 * @author Abhishek Ravi Chandran
	 *
	 */
	class YearData {
		YearData() {
			vals = new ArrayList<Carrier>();
		}

		List<Carrier> vals;
	}

	/**
	 * carrier object to hold projected price.
	 * @author Abhishek Ravi Chandran
	 *
	 */
	class Carrier {
		String name;
		float price;
	}
}
