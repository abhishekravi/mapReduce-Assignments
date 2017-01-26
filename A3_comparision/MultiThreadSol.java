import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

/**
 * File processing with Threads.
 * 
 * @author Abhishek Ravi Chandran
 * @author Chinmayee Vaidya
 */
public class MultiThreadSol {

	// Object to hold data
	final static PlainDataHolder DATAHOLDER = new PlainDataHolder();
	// DATAMAP variable will be by updated all the threads. But each of them
	// will place their value is a separate bucket ensuring there are not
	// concurrency issues.
	final static Map<Integer, PlainDataHolder.Data> DATAMAP = new HashMap<Integer, PlainDataHolder.Data>();
	// mode tells the program what to calculate
	static String MODE;

	/**
	 * main method.
	 * 
	 * @param args
	 *            arg1 -mn calculate mean -md calculate median -fm fast median
	 *            calculation arg2 -input path
	 */

	public static void main(String[] args) {

		// holds the directory path
		MODE = args[Constants.ARG3];
		String dirPath = args[Constants.ARG1].split("=")[1];
		// get list of files
		List<String> files = Util.processDir(dirPath);

		// number of threads equals to number of files
		final int THREADPOOL = files.size();

		Thread[] threads = new Thread[THREADPOOL];

		// Create a thread to process each file
		for (int i = 0; i < THREADPOOL; i++) {
			threads[i] = new Thread(new Processor(files.get(i), i));
		}

		// start all the threads
		for (Thread t : threads) {
			t.start();
		}
		PlainDataHolder.Data data = DATAHOLDER.getNewData();

		// all threads will join the main thread once they are finished
		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		consolidateData(data);
		// calculate and output average and median
		PlainDataHolder.calculateStat(data, MODE);

	}

	/**
	 * method that will consolidate all the data from different files.
	 * 
	 * @param data
	 *            data container for data from all files.
	 */
	private static void consolidateData(PlainDataHolder.Data data) {
		for (Entry<Integer, PlainDataHolder.Data> d : DATAMAP.entrySet()) {
			for (String k : d.getValue().records.keySet()) {
				if (!d.getValue().records.get(k).prices.isEmpty()) {
					if (data.records.get(k) == null) {
						data.records.put(k, d.getValue().records.get(k));
					} else {
						data.records.get(k).count += d.getValue().records
								.get(k).count;
						data.records.get(k).totalPrice += d.getValue().records
								.get(k).totalPrice;
						data.records.get(k).prices.addAll(d.getValue().records
								.get(k).prices);
					}
				}
			}
		}
	}

	/**
	 * Thread class that will process each file.
	 * 
	 * @author Abhishek Ravi Chandran
	 *
	 */
	static class Processor implements Runnable {
		// file to process
		String file;
		// thread id
		int id;

		Processor(String file, int id) {
			this.file = file;
			this.id = id;
		}

		@Override
		public void run() {
			processData(this.file);
		}

		/**
		 * method to process the airline data.
		 * 
		 * @param fileName
		 *            name of the file to be processed
		 */
		public void processData(String fileName) {
			InputStream fileStream;
			Reader decoder;
			try {
				// file to be read
				fileStream = new FileInputStream(fileName);
				// creating gzip stream to read the gzip file
				InputStream gzipStream = new GZIPInputStream(fileStream);
				// defining the encoding
				decoder = new InputStreamReader(gzipStream, "UTF-8");
				BufferedReader buffer = new BufferedReader(decoder);
				String line = "";
				// getting the headers from the file from line 1
				buffer.readLine();
				PlainDataHolder.Data data = DATAHOLDER.getNewData();
				RecordData recordData = new RecordData();

				// reading the data from the file, line by line

				while ((line = buffer.readLine()) != null) {
					try {
						Parser.getRecordData(line, recordData);
						if (DataValidator.isGoodRecord(recordData)) {
							String key = recordData.getMonth() + " "
									+ recordData.getCarrier();
							if (data.records.get(key) == null)
								data.records.put(key,
										DATAHOLDER.getNewCarrier());
							PlainDataHolder.Carrier carr = data.records
									.get(key);
							carr.count++;
							carr.prices.add(recordData.getAvgTicketPrice());
							carr.totalPrice += recordData.getAvgTicketPrice();
							carr.month = recordData.getMonth();
						}
					} catch (BadDataException be) {
						continue;
					}
				}
				buffer.close();
				// updating the global data variable
				DATAMAP.put(id, data);
			} catch (FileNotFoundException e) {
				new Error(e);
			} catch (UnsupportedEncodingException e) {
				new Error(e);
			} catch (IOException e) {
				new Error(e);
			}
		}

	}

}
