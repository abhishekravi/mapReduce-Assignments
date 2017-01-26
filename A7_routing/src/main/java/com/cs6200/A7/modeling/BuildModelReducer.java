package com.cs6200.A7.modeling;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cs6200.tools.Utils;

import quickml.data.instances.ClassifierInstance;
import quickml.supervised.ensembles.randomForest.randomDecisionForest.RandomDecisionForest;
import quickml.supervised.ensembles.randomForest.randomDecisionForest.RandomDecisionForestBuilder;
import quickml.supervised.tree.decisionTree.DecisionTreeBuilder;

/**
 * 
 * @author Chintan Pathak, Chinmayee Vaidya
 *
 */

/**
 * The reducer class for the modeling phase 
 * This class does the following things: 
 * 1) It gets the details about each flight
 * 2) It creates a dataset using those values that is then 
 *    fed to the randomForest library, which creates a prediction model 
 *    using the generated dataset 
 * 3) It then writes the randomForest object for this particular airline 
 *    carrier for the particular month to a file that is 
 *    named as <CarrierCode_Month> 
 * 4) This file is then used for making predictions for test data in the next phase
 */
public class BuildModelReducer extends Reducer<Text, ModelingCompositeValue, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<ModelingCompositeValue> values,
			Reducer<Text, ModelingCompositeValue, Text, Text>.Context context)
					throws IOException, InterruptedException {

		List<ClassifierInstance> dataset = Utils.loadDataSet(values);
		if (dataset != null) {
			RandomDecisionForest randomForest = new RandomDecisionForestBuilder<ClassifierInstance>(
					new DecisionTreeBuilder<ClassifierInstance>()).buildPredictiveModel(dataset);

			Utils.serializeObject(key.toString(), randomForest, context.getConfiguration());
		}
	}
}
