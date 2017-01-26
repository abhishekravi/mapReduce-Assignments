package com.cs6200.A7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.cs6200.A7.connections.FindConnectionsJob;
import com.cs6200.A7.modeling.ModelingJob;
import com.cs6200.A7.prediction.PredictionJob;
import com.cs6200.A7.validation.Validate;
import com.cs6200.tools.Utils;

/**
 * The main class: 
 * This class does the following thing 
 * 1) It takes in a set of arguments to the program and 
 * 	  parses them to be correct/incorrect with
 *    appropriate message in case of error 
 * 2) It chooses the mode of the program
 *    based on the passed arguments
 *        - Modeling
 *        - Prediction
 *        - Validation
 *        - Connections
 * 3) The modeling job builds the prediction model
 * 4) The prediction job uses the built models to predict
 *    any test input that is given to the program
 * 5) The validation application validates the output of the 
 *    prediction job against the validation file and 
 *    outputs the statistics
 * 6) The connections job finds all the possible 
 *    connections from the input files given
 *    (in our case, the training and the testing data)  
 * 
 * @author Chintan Pathak, Abhishek Ravi Chandran
 * @author Chinmayee Vaidya, Mania Abdi
 */

public class A7 {

	public static void main(String[] args) throws Exception {

		if (args[0].equalsIgnoreCase("modeling")) {
			System.exit(ToolRunner.run(new Configuration(), new ModelingJob(), args));
		} else if (args[0].equalsIgnoreCase("prediction")) {
			System.exit(ToolRunner.run(new Configuration(), new PredictionJob(), args));
		} else if (args[0].equalsIgnoreCase("validation")) {
			Utils.runApp(new Validate(), args[1], args[2]);
		} else if (args[0].equalsIgnoreCase("connections")) {
			System.exit(ToolRunner.run(new FindConnectionsJob(), args));
		} else {
			System.err.println("Invalid type to the program");
			System.exit(-1);
		}
	}
}
