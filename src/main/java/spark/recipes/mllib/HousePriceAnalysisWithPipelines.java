package spark.recipes.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
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

import static org.apache.spark.sql.functions.col;

public class HousePriceAnalysisWithPipelines {

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

        // Creating a new variable
        dataSet = dataSet
                .withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living")))
                .withColumnRenamed("price", "label");

        // Split data intro train, test and holdOut
        Dataset<Row>[] dataSplits = dataSet.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainAndTest = dataSplits[0];
        Dataset<Row> holdOut = dataSplits[1];

        // Dealing with non numerical values
        StringIndexer conditionIndexer = new StringIndexer()
                .setInputCol("condition")
                .setOutputCol("conditionIndex");

        StringIndexer gradeIndexer = new StringIndexer()
                .setInputCol("grade")
                .setOutputCol("gradeIndex");

        StringIndexer zipcodeIndexer = new StringIndexer()
                .setInputCol("zipcode")
                .setOutputCol("zipcodeIndex");

        OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
        encoder.setInputCols(new String[]{"conditionIndex", "gradeIndex", "zipcodeIndex"});
        encoder.setOutputCols(new String[]{"conditionVector", "gradeVector", "zipcodeVector"});

        // Assembling a Vector of Features
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living", "sqft_above_percentage", "floors", "conditionVector", "gradeVector", "zipcodeVector", "waterfront"})
                .setOutputCol("features");

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

        Pipeline pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{conditionIndexer, gradeIndexer, zipcodeIndexer, encoder, assembler, trainValidationSplit});

        /*
         * 5. Model fitting
         */
        PipelineModel pipelineModel = pipeline.fit(trainAndTest);
        TrainValidationSplitModel allModels = (TrainValidationSplitModel) pipelineModel.stages()[5];
        LinearRegressionModel bestModel = (LinearRegressionModel) allModels.bestModel();

        /*
         * 6. Evaluating the model
         */
        // The model fitting is performed only in the trainAndTest data set, so in order to evaluate the model with the
        // holdOut data, we have to call transform in the pipelineModel with the holdOut data and drop the column
        // "prediction" in order to be able to call evaluate, otherwise is going to throw an error saying that the
        // column "prediction" already exists.
        Dataset<Row> holdOutResults = pipelineModel.transform(holdOut).drop("prediction");

        double rmseTrainAndTest = bestModel.summary().rootMeanSquaredError(); // smaller the better
        double r2TrainAndTest = bestModel.summary().r2(); // closer to 1 the better
        double rmseHoldOut = bestModel.evaluate(holdOutResults).rootMeanSquaredError();
        double r2HoldOut = bestModel.evaluate(holdOutResults).r2();

        System.out.println("Intercept = " + bestModel.intercept() + "\ncoefficients = " + bestModel.coefficients());
        System.out.println("[trainAndTest] RMSE = " + rmseTrainAndTest + "\n[trainAndTest] R2 = " + r2TrainAndTest);
        System.out.println("[holdOut] RMSE = " + rmseHoldOut + "\n[holdOut] R2 = " + r2HoldOut);

        double chosenRegParam = bestModel.getRegParam();
        double chosenElasticNetParam = bestModel.getElasticNetParam();

        System.out.println("chosenRegParam = " + chosenRegParam + "\nchosenElasticNetParam = " + chosenElasticNetParam);
    }
}
