package spark.examples.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * In this example we use Decision Trees and Random Forest models to predict if customers will pay after free trial,
 * using SparkSQL and ml.
 */
public class PredictingIfCustomerWillPayAfterFreeTrial {

    public static UDF1<String,String> countryGrouping = new UDF1<String,String>() {
        @Override
        public String call(String country) throws Exception {
            List<String> topCountries =  Arrays.asList("GB","US","IN","UNKNOWN");
            List<String> europeanCountries =  Arrays.asList("BE","BG","CZ","DK","DE","EE","IE","EL","ES","FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI","SE","CH","IS","NO","LI","EU");

            if (topCountries.contains(country)) return country;
            if (europeanCountries .contains(country)) return "EUROPE";
            else return "OTHER";
        }
    };

    public static void main(String[] args) {

        /*
         * Initialization
         */
        // The following property has to be set only on windows environments
        System.setProperty("hadoop.home.dir", "d:/dev/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession session = SparkSession.builder()
                .appName("HousePriceAnalysis")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/") // Windows
//                .config("spark.sql.warehouse.dir", "file:/Users/ivan/Desktop/tmp/") // macOS
                .getOrCreate();

        session.udf().register("countryGrouping", countryGrouping, DataTypes.StringType);

        Dataset<Row> dataSet = session.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/vppFreeTrials.csv")
                .withColumn("country", callUDF("countryGrouping", col("country")))
                .withColumn("label", when(col("payments_made").geq(1), lit(1)).otherwise(lit(0)));

        StringIndexer countryIndexer = new StringIndexer()
                .setInputCol("country")
                .setOutputCol("countryIndex");
        dataSet = countryIndexer.fit(dataSet).transform(dataSet);

        // This code snippet serves to see which country corresponds to which countryIndex
        new IndexToString()
                .setInputCol("countryIndex")
                .setOutputCol("countryName")
                .transform(dataSet.select("countryIndex").distinct())
                .show();

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"countryIndex", "rebill_period", "chapter_access_count", "seconds_watched"})
                .setOutputCol("features");

        Dataset<Row> inputModelData = assembler.transform(dataSet).select("label", "features");

        Dataset<Row>[] trainingAndHoldOut = inputModelData.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingAndTest = trainingAndHoldOut[0];
        Dataset<Row> holdOut = trainingAndHoldOut[1];

        DecisionTreeClassifier classifier = new DecisionTreeClassifier().setMaxDepth(5);
        DecisionTreeClassificationModel model = classifier.fit(trainingAndTest);

        Dataset<Row> predictions = model.transform(holdOut);
        predictions.show();

        System.out.println(model.toDebugString());

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");

        System.out.println("The accuracy of the model is " + evaluator.evaluate(predictions));

        RandomForestClassifier rfClassifier = new RandomForestClassifier().setMaxDepth(5);
        RandomForestClassificationModel rfModel = rfClassifier.fit(trainingAndTest);

        Dataset<Row> rfPredictions = rfModel.transform(holdOut);
        rfPredictions.show();

        System.out.println(rfModel.toDebugString());

        System.out.println("The accuracy of the forest model is " + evaluator.evaluate(rfPredictions));
    }
}
