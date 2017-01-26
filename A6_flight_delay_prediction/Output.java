import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import com.opencsv.CSVReader;

/**
 * class to create confusion matrix.
 * @author Chinmayee Vaidya
 * @author Abhishek Ravi Chandran
 */
public class Output {
	static Map<String, String> vdata = new HashMap<String, String>();
	static int true_true;
	static int true_false;
	static int false_true;
	static int false_false;

	public static void main(String[] args) throws IOException {
		String ip = args[0];
		String op = args[1];
		readFile(ip, vdata);
		createConfusionMatrix(op);
		StringBuilder sb = new StringBuilder();
		sb.append("       TRUE     FALSE \n").
		append("TRUE   "+ true_true + "  " + true_false+"\n").
		append("FALSE  " + false_true + "  " + false_false+"\n");
		double acc = (double)(true_true + false_false)/
				(double)(true_true + false_false+false_true + true_false);
		
		PrintWriter writer = new PrintWriter("op.txt", "UTF-8");
		writer.println(sb.toString());
		writer.println("accuracy:" + acc);
		writer.close();
	}

	/**
	 * method to read validate file
	 * @param ip
	 * path of file
	 * @param vdata
	 * map to store data
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void readFile(String ip, Map<String, String> vdata)
			throws FileNotFoundException, IOException {
		File folder = new File(ip);
		File[] files = folder.listFiles();
		for (File file : files) {
			readCSVFile(file, vdata);
		}
	}

	/**
	 * method to read csv.
	 * @param file
	 * file
	 * @param map
	 * map to store it in
	 */
	private static void readCSVFile(File file, Map<String, String> map) {
		CSVReader reader;
		try {
			reader = new CSVReader(new FileReader(file));
			String c[];
			while ((c = reader.readNext()) != null) map.put(c[0], c[1]);
			reader.close();
		} catch (Exception e) {}

	}

	/**
	 * method to create confusion matrix.
	 * @param op
	 * output files path
	 * @throws FileNotFoundException
	 */
	private static void createConfusionMatrix(String op) throws FileNotFoundException {
		File folder = new File(op);
		File[] files = folder.listFiles();
		CSVReader reader;
		for (File file : files) {
			reader = new CSVReader(new FileReader(file));
			String c[];
			try {
				while ((c = reader.readNext()) != null) {
					if (vdata.get(c[0]) != null) {
						if (vdata.get(c[0]).equals(c[1])) {
							if (c[1].equals("TRUE"))
								true_true++;
							else
								false_false++;
						} else {
							if (c[1].equals("TRUE"))
								false_true++;
							else
								true_false++;
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}