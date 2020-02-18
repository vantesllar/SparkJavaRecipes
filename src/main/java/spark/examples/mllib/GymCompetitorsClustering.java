package spark.examples.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * In this example we make a very simple linear regression model to show how to use very few
 * features of the MLlib API. One important thing that we do wrong here is using the same data
 * to fit the model and make the prediction, as we should be splitting it in train and test data
 * sets.
 */
public class GymCompetitorsClustering {

    public static void main(String[] args) {

        // The following property has to be set only on windows environments
        System.setProperty("hadoop.home.dir", "d:/dev/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession session = SparkSession.builder()
                .appName("GymCompetitors")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///d:/tmp/") // Windows
//                .config("spark.sql.warehouse.dir", "file:/Users/ivan/Desktop/tmp/") // macOS
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

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"GenderVector", "Age", "Height", "Weight", "NoOfReps"})
                .setOutputCol("features");

        Dataset<Row> inputModelData = assembler.transform(dataSet).select("features");

        // Creating model:
        KMeans kMeans = new KMeans();

        for(int k=2; k<=8; k++){

            // Creating model:
            kMeans.setK(k);
            KMeansModel model = kMeans.fit(inputModelData);

            // Making a prediction:
            Dataset<Row> predictions = model.transform(inputModelData);
            predictions.show();

            // Inspecting cluster centers:
            Vector[] centers = model.clusterCenters();
            for(Vector vector: centers){
                System.out.println("[clusters=" + k + "] vector=" + vector);
            }

            // Getting count of elements per cluster:
            predictions.groupBy("prediction").count().show();

            // Evaluating clusters
            /*
             We want to minimize SSE, so ideally we will print this value over all iterations, searching for the
             elbow in the graph
             */
            System.out.println("[clusters=" + k + "] SSE is " + model.computeCost(inputModelData));

            ClusteringEvaluator evaluator = new ClusteringEvaluator();
            System.out.println("[clusters=" + k + "] Silhouette with squared euclidean distance is " + evaluator.evaluate(predictions));
        }
    }
}
