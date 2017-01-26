package com.cs6270.A8;
/**
 * Linear regression operations.
 * 
 * @author Abhishek Ravi Chandran
 *
 */
public class LinearRegression {

	/**
	 * method to calculate slope.
	 * 
	 * @param NumberOfPeriods
	 *            number of values
	 * @param SumOfXValues
	 *            sum of x
	 * @param SumOfYValues
	 *            sum of y
	 * @param SumOfXYValues
	 *            sum of xy
	 * @param SumOfXXValues
	 *            sum of xx
	 * @return slope
	 */
	public static double calculateSlope(long NumberOfPeriods,
			double SumOfXValues, double SumOfYValues, double SumOfXYValues,
			double SumOfXXValues) {
		double slope = ((NumberOfPeriods * SumOfXYValues) - (SumOfXValues * SumOfYValues))
				/ ((NumberOfPeriods * SumOfXXValues) - (Math.pow(SumOfXValues,
						2)));
		return slope;
	}

	/**
	 * method to calculate intercept.
	 * 
	 * @param NumberOfPeriods
	 *            num of values
	 * @param SumOfXValues
	 *            sum of x
	 * @param SumOfYValues
	 *            sum of y
	 * @param Slope
	 *            slope
	 * @return intercept
	 */
	public static double calculateIntercept(long NumberOfPeriods,
			double SumOfXValues, double SumOfYValues, double Slope) {

		double intercept = (SumOfYValues - (Slope * SumOfXValues))
				/ NumberOfPeriods;
		return intercept;
	}

	/**
	 * method to calculate projected value.
	 * 
	 * @param Slope
	 *            slope
	 * @param Intercept
	 *            intercept
	 * @param TargetPeriod
	 *            time
	 * @return predicted cost
	 */
	public static double calculateProjectedScoreForTargetPeriod(double Slope,
			double Intercept, double TargetPeriod) {
		double ProjectedScore = Intercept + (Slope * TargetPeriod);
		return ProjectedScore;
	}
}
