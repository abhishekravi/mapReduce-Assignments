package com.cs6270.A8;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Serializable;
import scala.Tuple2;

/**
 * 
 * @author Abhishek Ravi Chandran
 * 
 * Main driver class.
 *
 */
public class A8 {

	/**
	 * This method reads the files and filters out unwanted data.
	 * @param args : Command line arguments for input, output directory and mode
	 */
	public static void main(String[] args) {
		if(args.length != Constants.MRARGSIZE){
			System.out.println("usage: <input dir> <output dir> <mode>");
			System.exit(0);
		}
		
		Map<String, Data> values = new HashMap<String, Data>();
		Map<String, Float> reg = new HashMap<String, Float>();
		String input = args[Constants.ARG1].split("=")[1];
		String output = args[Constants.ARG2].split("=")[1];
		String mode = args[Constants.ARG3].split("=")[1];
		FindCheapest fc = new FindCheapest();
		
		//Initialize Spark Context
		SparkConf conf;
		if(mode.equals("cloud"))
			conf = new SparkConf().setAppName("A8").set("spark.executor.memory", "5g");
		else
			conf = new SparkConf().setAppName("A8").set("spark.executor.memory", "5g").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		// mapper to get all the values that are needed into an RDD
		JavaPairRDD<String, FData> data = mapper(jsc,input);
		//persist the data so that we do not have to read the files again
		data.persist(StorageLevel.MEMORY_AND_DISK_SER());
		//get all the aggregated values for performing calculations
		values = getCalcMap(data);
		//perform regression calculations for n=1
		calculateRegression(values,reg,1);
		final String c1 = fc.findCheapest(reg);
		//perform regression calculations for n=200
		calculateRegression(values,reg,200);
		final String c200 = fc.findCheapest(reg);
		
		//generate output for n=1
		writeToFile(data,c1,output+"_1");
		//generate output for n=200
		writeToFile(data,c200,output+"_200");
		
		jsc.close();
	}
	
	/**
	 * method to calculate weekly averages and write to output.
	 * @param data
	 * main data rdd that has all the records
	 * @param carrier
	 * carrier to filter
	 * @param output
	 * output directory
	 */
	@SuppressWarnings("serial")
	private static void writeToFile(JavaPairRDD<String, FData> data, final String carrier,
			String output) {
		//filter out unwanted carriers
		JavaPairRDD<String, Float> op = data.filter(new Function<Tuple2<String,FData>,Boolean>(){

			public Boolean call(Tuple2<String, FData> v1) throws Exception {
				return v1._1.split("\\s+")[0].equals(carrier);
			}
			
		})
		//create key with carrier,year_week and values as the price
		.mapToPair(new PairFunction<Tuple2<String,FData>,String,Float>(){
			public Tuple2<String, Float> call(Tuple2<String, FData> t)
					throws Exception {
				FData f = t._2;
				String key = f.carrier + "," + f.year + "_" + Utils.getWeek(f.fdate);
				return new Tuple2<String, Float>(key,f.price);
			}
			
		})//add all the values that fall under the same carrier,year_week to a list with the same key
		.aggregateByKey(new ArrayList<Float>(),
				new Function2<List<Float>,Float,List<Float>>(){

			public List<Float> call(List<Float> v1, Float v2)
					throws Exception {
				v1.add(v2);
				return v1;
			}
			
		}, new Function2<List<Float>,List<Float>,List<Float>>(){

			public List<Float> call(List<Float> v1,
					List<Float> v2) throws Exception {
				v1.addAll(v2);
				return v1;
			}
		})
		//calculate the median for all the values collected above for year_week bucket
		.mapToPair(new PairFunction<Tuple2<String,List<Float>>,String,Float>(){

			public Tuple2<String, Float> call(Tuple2<String, List<Float>> v1)
					throws Exception {
				return new Tuple2<String, Float>(v1._1,Utils.fastMedian(v1._2())) ;
			}
			
		});
		op.saveAsTextFile(output);
	}

	/**
	 * method to calculate the needed values to perform linear regression.	
	 * @param data
	 * main data RDD
	 * @return
	 * return a pair RDD with key as the carrier and year, value as Data class(holds calculated values).
	 */
	private static Map<String, Data> getCalcMap(JavaPairRDD<String, FData> data) {
		@SuppressWarnings("serial")
		//create a Data object for each record
		JavaPairRDD<String, Data> cData = data.mapToPair(new PairFunction<Tuple2<String,FData>,String,Data>(){
			public Tuple2<String, Data> call(Tuple2<String, FData> t)
					throws Exception {
				FData f = t._2;
				Data d = new Data();
				d.tDuraiton+=f.etime;
				d.tPrice+=f.price;
				d.num++;
				d.sumxx+=f.etime*f.etime;
				d.sumxy+=f.price*f.etime;
				return new Tuple2<String, Data>(t._1,d);
			}
			
		})
		//combine all the Data values for the same carrier and year
		.reduceByKey(new Function2<Data,Data,Data>(){

			public Data call(Data v1, Data v2) throws Exception {
				v1.num += v2.num;
				v1.tDuraiton += v2.tDuraiton;
				v1.tPrice += v2.tPrice;
				v1.sumxx += v2.sumxx;
				v1.sumxy += v2.sumxy;
				return v1;
			}
			
		});
		return cData.collectAsMap();
	}

	/**
	 * method to perform linear regression.
	 * @param values
	 * map holding all values
	 * @param reg
	 * map that will hold the calculated regression value
	 * @param v
	 * n value
	 */
	private static void calculateRegression(Map<String, Data> values,Map<String, Float> reg, int v) {
		Data d;
		for(Entry<String, Data> e : values.entrySet()){
			d = e.getValue();
			double slope = LinearRegression.calculateSlope(d.num, d.tDuraiton, d.tPrice, d.sumxy,d.sumxx);
			double intercept = LinearRegression.calculateIntercept(d.num, d.tDuraiton, d.tPrice, slope);
			reg.put(e.getKey(),(float) LinearRegression.calculateProjectedScoreForTargetPeriod(slope, intercept, v));
		}
	}
	
	/**
	 * mapping method that constructs the RDD by ignoring bad records
	 * @param jsc
	 * java spark context
	 * @param input
	 * input folder path
	 * @param record
	 * RecordData object to hold data
	 * @return
	 * JavaPairRDD
	 */
	@SuppressWarnings("serial")
	private static JavaPairRDD<String, FData> mapper(JavaSparkContext jsc, String input){
		final RecordData record = new RecordData();
		return jsc.textFile(input , 1)
		// Filter to get only good data
		.filter(new Function<String, Boolean>() {
			public Boolean call(String line) throws Exception {
				try{
				record.fill(Utils.parseCSV(line), 0);
				} catch (BadDataException e){return false;}
				return record.isSane();
			}
		}) 
		.map(new Function<String, FData>() {
			public FData call(String line) throws Exception {
				record.fill(Utils.parseCSV(line), 0);
				return new FData(record);
			}
		})
		//create pair with key as carrier,year
		.mapToPair(new PairFunction<FData, String, FData>(){

			public Tuple2<String, FData> call(FData f) throws Exception {
				return new Tuple2<String, FData>((f.carrier+" "+f.year),f);
			}
			
		});
	}

	/**
	 * Class to hold calculated sum values.
	 * @author Abhishek Ravi Chandran
	 *
	 */
	static class Data implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		long tDuraiton;
		long tPrice;
		int num;
		long sumxy;
		long sumxx;
	}

}
