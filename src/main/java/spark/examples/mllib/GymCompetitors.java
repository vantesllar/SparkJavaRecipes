package spark.examples.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * In this example we make a very simple linear regression model to show how to use very few
 * features of the MLlib API. One important thing that we do wrong here is using the same data
 * to fit the model and make the prediction, as we should be splitting it in train and test data
 * sets.
 */
public class GymCompetitors {

    public static void main(String[] args) {

        // The following property has to be set only on windows environments
        System.setProperty("hadoop.home.dir", "d:/dev/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession session = SparkSession.builder()
                .appName("GymCompetitors")
                .master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///d:/tmp/") // Windows
                .config("spark.sql.warehouse.dir", "file:/Users/ivan/Desktop/tmp/") // macOS
                .getOrCreate();

        Dataset<Row> dataSet = session.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/GymCompetition.csv");

        dataSet.printSchema();

        StringIndexer genderIndexer = new StringIndexer();
        genderIndexer.setInputCol("Gender");
        genderIndexer.setOutputCol("GenderIndex");
        dataSet = genderIndexer.fit(dataSet).transform(dataSet);

        OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator();
        genderEncoder.setInputCols(new String[]{"GenderIndex"});
        genderEncoder.setOutputCols(new String[]{"GenderVector"});
        dataSet = genderEncoder.fit(dataSet).transform(dataSet);

        dataSet.show();

        // Assembling a Vector of Features
        VectorAssembler assembler = new VectorAssembler();
        assembler.setInputCols(new String[] {"Age", "Height", "Weight", "GenderVector"});
        assembler.setOutputCol("features");
        Dataset<Row> dataSetWithFeatures = assembler.transform(dataSet);

        // Preparing model input data
        Dataset<Row> modelInputData = dataSetWithFeatures
                .select("NoOfReps", "features")
                .withColumnRenamed("NoOfReps", "label");

        // Preparing and fitting model
        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel model = linearRegression.fit(modelInputData);

        System.out.println("Intercept = " + model.intercept() + "\ncoefficients = " + model.coefficients());

        // Making a prediction
        Dataset<Row> prediction = model.transform(modelInputData);

        prediction.show();
    }
}
