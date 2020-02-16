package spark.recipes.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class HousePriceAnalysis {

    public static void main(String[] args) {

        /*
         * Initialization
         */
        // The following property has to be set only on windows environments
        System.setProperty("hadoop.home.dir", "D:/cursos/spark/Practicals/winutils-extra/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession session = SparkSession.builder()
                .appName("HousePriceAnalysis")
                .master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///c:/tmp/") // Windows
                .config("spark.sql.warehouse.dir", "file:/Users/ivan/Desktop/tmp/") // macOS
                .getOrCreate();

        Dataset<Row> dataSet = session.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/kc_house_data.csv");

//        dataSet.printSchema();
//        dataSet.describe().show();

        /*
         * 2. Feature selection
         */
        // We choose to eliminate these variables from this analysis after visual inspection
        dataSet = dataSet.drop("id", "date", "view", "yr_renovated", "lat", "long");

        // Showing variables correlation with the price
        for (String col : dataSet.columns()) {
            double correlation = dataSet.stat().corr("price", col);
            System.out.println("Correlation between price and " + col + " is " + correlation);
        }

        // We remove this variables after knowing their correlation with the price
        dataSet = dataSet.drop("sqft_lot", "sqft_lot15", "yr_built", "sqft_living15");

        // Showing variables correlation among them
        for (String col1 : dataSet.columns()) {
            for (String col2 : dataSet.columns()) {
                double correlation = dataSet.stat().corr(col1, col2);
                System.out.println("Correlation between " + col1 + " and " + col2 + " is " + correlation);
            }
        }

        // Creating a new variable
        dataSet = dataSet.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living")));

        // Dealing with non numerical values
        StringIndexer conditionIndexer = new StringIndexer()
                .setInputCol("condition")
                .setOutputCol("conditionIndex");
        dataSet = conditionIndexer.fit(dataSet).transform(dataSet);

        dataSet = new StringIndexer()
                .setInputCol("grade")
                .setOutputCol("gradeIndex")
                .fit(dataSet).transform(dataSet);

        dataSet = new StringIndexer()
                .setInputCol("zipcode")
                .setOutputCol("zipcodeIndex")
                .fit(dataSet).transform(dataSet);

        OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
        encoder.setInputCols(new String[]{"conditionIndex", "gradeIndex", "zipcodeIndex"});
        encoder.setOutputCols(new String[]{"conditionVector", "gradeVector", "zipcodeVector"});
        dataSet = encoder.fit(dataSet).transform(dataSet);

        dataSet.show();

        /*
         * 3. Data preparation
         */
        // Assembling a Vector of Features
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living", "sqft_above_percentage", "floors", "conditionVector", "gradeVector", "zipcodeVector", "waterfront"})
                .setOutputCol("features");

        // Preparing model input data
        Dataset<Row> modelInputData = assembler.transform(dataSet)
                .select("price", "features")
                .withColumnRenamed("price", "label");

//        modelInputData.show();

        // Split data intro tran, test and holdOut
        Dataset<Row>[] dataSplits = modelInputData.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainAndTest = dataSplits[0];
        Dataset<Row> holdOut = dataSplits[1];

        /*
         * 4. Choosing model fitting parameters
         */
        LinearRegression linearRegression = new LinearRegression();

        ParamMap[] paramMaps = new ParamGridBuilder()
                .addGrid(linearRegression.regParam(), new double[]{0.01, 0.1, 0.5})
                .addGrid(linearRegression.elasticNetParam(), new double[]{0, 0.5, 1})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2")) // or rmse
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.8);

        /*
         * 5. Model fitting
         */
        TrainValidationSplitModel allModels = trainValidationSplit.fit(trainAndTest);
        LinearRegressionModel bestModel = (LinearRegressionModel) allModels.bestModel();

        /*
         * 6. Evaluating the model
         */
        double rmseTrainAndTest = bestModel.summary().rootMeanSquaredError(); // smaller the better
        double r2TrainAndTest = bestModel.summary().r2(); // closer to 1 the better
        double rmseHoldOut = bestModel.evaluate(holdOut).rootMeanSquaredError();
        double r2HoldOut = bestModel.evaluate(holdOut).r2();

        System.out.println("Intercept = " + bestModel.intercept() + "\ncoefficients = " + bestModel.coefficients());
        System.out.println("[trainAndTest] RMSE = " + rmseTrainAndTest + "\n[trainAndTest] R2 = " + r2TrainAndTest);
        System.out.println("[holdOut] RMSE = " + rmseHoldOut + "\n[holdOut] R2 = " + r2HoldOut);

        double chosenRegParam = bestModel.getRegParam();
        double chosenElasticNetParam = bestModel.getElasticNetParam();

        System.out.println("chosenRegParam = " + chosenRegParam + "\nchosenElasticNetParam = " + chosenElasticNetParam);


        // Making a prediction
//        Dataset<Row> prediction = model.transform(holdOut);
//        prediction.show();

    }
}
