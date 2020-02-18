package spark.examples.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;


/**
 * In this example we use a Logistic Regression model to predict how likely are customers to stop watching videos next
 * month, using SparkSQL and ml.
 */
public class PredictingIfCustomerWillStopWatchingVideosNextMonth {

    /*
     * Instructions:
     * 1. Filter out all records where the customer has cancelled
     * 2. Replace any nulls in firstSub, all_time_views and last_month_views by 0
     * 3. Replace any value in next_month_views by 0 if it is bigger than 1 and 1 otherwise, as we want to predict if
     *    they WILL STOP WATCHING VIDEOS, so next_month_views = 0 is our positive case
     * 4. Build a Linear Regression model using all available fields other than the date (4.0)
     *  4.1. Encode all categorical data into vectors
     *  4.2. Use pipelines if desired
     *  4.3. Use a range of model fitting parameters to minimize r2
     *  4.4. Use 90/10 splits for holdout and for test
     */

    public static void main(String[] args) {

        /*
         * Initialization
         */
        // The following property has to be set only on windows environments
        System.setProperty("hadoop.home.dir", "d:/dev/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession session = SparkSession.builder()
                .appName("PredictingHowManyVideosWillCustomerWatchNextMonth")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/") // Windows
//                .config("spark.sql.warehouse.dir", "file:///Users/ivan/Desktop/tmp/") // macOS
                .getOrCreate();

        Dataset<Row> dataSet = session.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/vppChapterViews/*.csv")
                .filter(col("is_cancelled").equalTo(false)) // 1
                .drop("is_cancelled", "observation_date") // 1 & 3.0
                .withColumn("firstSub", when(col("firstSub").isNull(), 0).otherwise(col("firstSub"))) // 2
                .withColumn("all_time_views", when(col("all_time_views").isNull(), 0).otherwise(col("all_time_views"))) // 2
                .withColumn("last_month_views", when(col("last_month_views").isNull(), 0).otherwise(col("last_month_views"))) // 2
                .withColumn("next_month_views", when(col("next_month_views").$greater(0), 0).otherwise(1)); // 3

//        dataSet.show();
//        dataSet.printSchema();
//        dataSet.describe().show();

        // 4.1
        StringIndexer paymentMethodTypeIndexer = new StringIndexer()
                .setInputCol("payment_method_type")
                .setOutputCol("paymentMethodTypeIndex");
        dataSet = paymentMethodTypeIndexer.fit(dataSet).transform(dataSet);

        StringIndexer countryIndexer = new StringIndexer()
                .setInputCol("country")
                .setOutputCol("countryIndex");
        dataSet = countryIndexer.fit(dataSet).transform(dataSet);

        StringIndexer rebillPeriodInMonthsIndexer = new StringIndexer()
                .setInputCol("rebill_period_in_months")
                .setOutputCol("rebillPeriodInMonthsIndex");
        dataSet = rebillPeriodInMonthsIndexer.fit(dataSet).transform(dataSet);

        OneHotEncoderEstimator encoder = new OneHotEncoderEstimator()
                .setInputCols(new String[]{"paymentMethodTypeIndex", "countryIndex", "rebillPeriodInMonthsIndex"})
                .setOutputCols(new String[]{"paymentMethodTypeVector", "countryVector", "rebillPeriodInMonthsVector"});
        dataSet = encoder.fit(dataSet).transform(dataSet);

        // 4.3
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"firstSub", "age", "all_time_views", "last_month_views", "paymentMethodTypeVector", "countryVector", "rebillPeriodInMonthsVector"})
                .setOutputCol("features");

        Dataset<Row> modelInputData = assembler.transform(dataSet)
                .select("next_month_views", "features")
                .withColumnRenamed("next_month_views", "label");

        LogisticRegression logisticRegression = new LogisticRegression();

        ParamMap[] paramMaps = new ParamGridBuilder()
                .addGrid(logisticRegression.regParam(), new double[]{0.01, 0.1, 0.5, 0.7, 0.9})
                .addGrid(logisticRegression.elasticNetParam(), new double[]{0, 0.5, 1})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(logisticRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.9); // 4.4

        // 4.4
        Dataset<Row>[] dataSplits = modelInputData.randomSplit(new double[]{0.9, 0.1});
        Dataset<Row> trainAndTest = dataSplits[0];
        Dataset<Row> holdOut = dataSplits[1];

        TrainValidationSplitModel allModels = trainValidationSplit.fit(trainAndTest);
        LogisticRegressionModel bestModel = (LogisticRegressionModel) allModels.bestModel();

        /*
         * Evaluating the model
         */
        double accuracyTrainAndTest = bestModel.summary().accuracy();
        LogisticRegressionSummary summary = bestModel.evaluate(holdOut);
        double accuracyHoldOut = summary.accuracy();
        double truePositive = summary.truePositiveRateByLabel()[1];
        double falsePositive = summary.falsePositiveRateByLabel()[0];
        double trueNegative = summary.truePositiveRateByLabel()[0];
        double falseNegative = summary.falsePositiveRateByLabel()[1];
        double correctPositiveLikelihood = truePositive / (truePositive + falsePositive);
        double correctNegativeLikelihood = trueNegative / (trueNegative + falseNegative);

        System.out.println("Intercept = " + bestModel.intercept() + "\ncoefficients = " + bestModel.coefficients());
        System.out.println("[trainAndTest] accuracy = " + accuracyTrainAndTest);
        System.out.println("[holdOut] accuracy = " + accuracyHoldOut);
        System.out.println("[holdOut] Likelihood of customer stop watching videos = " + correctPositiveLikelihood);

        double chosenRegParam = bestModel.getRegParam();
        double chosenElasticNetParam = bestModel.getElasticNetParam();

        System.out.println("chosenRegParam = " + chosenRegParam + "\nchosenElasticNetParam = " + chosenElasticNetParam);

        bestModel.transform(holdOut).groupBy("label", "prediction").count().show();
    }
}
