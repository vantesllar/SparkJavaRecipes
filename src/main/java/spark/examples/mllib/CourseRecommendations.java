package spark.examples.mllib;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sha1;

public class CourseRecommendations {

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
                .csv("src/main/resources/VPPcourseViews.csv")
                .withColumn("proportionWatched", col("proportionWatched").multiply(100));

//        dataSet.groupBy("userId").pivot("courseId").sum("proportionWatched").show();

        // Creating model:
        ALS als = new ALS()
                .setMaxIter(10)
                .setRegParam(0.1)
                .setUserCol("userId")
                .setItemCol("courseId")
                .setRatingCol("proportionWatched");

        /*
        With ALS we do not need to assemble a features vector but we have to specify the users, items and ratings columns
        instead. We do not have to split data either.
        Setting cold start strategy to drop we avoid values that can't be provided any recommendation.
         */
        ALSModel model = als.fit(dataSet)
                .setColdStartStrategy("drop");

        // Making the actual recommendations (the predictions):
        Dataset<Row> userRecommendations = model.recommendForAllUsers(5);
        userRecommendations.show();

        // Example of getting some recommendations and displaying them
        List<Row> userRecommendationsList = userRecommendations.takeAsList(5);
        for(Row row : userRecommendationsList){
            int userId = row.getAs(0);
            String recommendations = row.getAs(1).toString();
            System.out.println("User " + userId + " we might want to recommend " + recommendations);
            System.out.println("Because the user has already watched:");
            dataSet.filter("userId = " + userId).show();
        }
    }
}
