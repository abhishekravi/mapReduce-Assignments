import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * 
 * @author Abhishek Ravi Chandran
 * @author Chinmayee Vaidya
 *
 */
public class SingleThreadSol {

	static String mode;
	final static PlainDataHolder DATAHOLDER = new PlainDataHolder();

	/**
	 * main method
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		SingleThreadSol mri = new SingleThreadSol();
		mode = args[Constants.ARG3];
		mri.processData(args[Constants.ARG1].split("=")[1]);
	}

	/**
	 * method to process the airline data.
	 * 
	 * @param fileName
	 *            name of the file to be processed
	 */
	public void processData(String dir) {
		InputStream fileStream;
		Reader decoder;
		try {
			// file to be read
			List<String> files = Util.processDir(dir);
			// fileStream = new FileInputStream(fileName);
			PlainDataHolder.Data data = DATAHOLDER.getNewData();
			// incrementing knum as this is a bad line
			BufferedReader buffer = null;
			RecordData recordData = new RecordData();
			// reading the data from the file, line by line
			for (String file : files) {
				fileStream = new FileInputStream(file);
				// creating gzip stream to read the gzip file
				InputStream gzipStream = new GZIPInputStream(fileStream);
				// defining the encoding
				decoder = new InputStreamReader(gzipStream, "UTF-8");
				buffer = new BufferedReader(decoder);
				String line = "";

				// getting the headers from the file from line 1
				line = buffer.readLine();
				while ((line = buffer.readLine()) != null) {
					try {
						Parser.getRecordData(line, recordData);
						if (DataValidator.isGoodRecord(recordData)) {
							String key = recordData.getMonth() + " "
									+ recordData.getCarrier();
							if (data.records.get(key) == null)
								data.records.put(key, DATAHOLDER.getNewCarrier());
							PlainDataHolder.Carrier carr = data.records.get(key);
							carr.count++;
							carr.month = recordData.getMonth();
							carr.totalPrice += recordData.getAvgTicketPrice();
							carr.prices.add(recordData.getAvgTicketPrice());
						}
					} catch (BadDataException be) {
						continue;
					}
				}
				buffer.close();
				
			}
			PlainDataHolder.calculateStat(data, mode);

		} catch (FileNotFoundException e) {
			new Error(e);
		} catch (UnsupportedEncodingException e) {
			new Error(e);
		} catch (IOException e) {
			new Error(e);
		}
	}



	

}
