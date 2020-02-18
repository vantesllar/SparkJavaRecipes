package spark.examples.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * In this example we get the average subject's score and its standard deviation per year,
 * using the select, groupBy, pivot and agg Dataset (SparkSQL) methods.
 */
public class ExamResults {

    public static void main(String[] args) {

        // The following property has to be set only on windows environments
        System.setProperty("hadoop.home.dir", "d:/dev/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession session = SparkSession.builder()
                .appName("learningSparkSQL")
                .master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///d:/tmp/") // Windows
                .config("spark.sql.warehouse.dir", "file:/Users/ivan/Desktop/tmp/") // macOS
                .getOrCreate();

        Dataset<Row> dataSet = session.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        Dataset<Row> results = dataSet
                .select(col("subject"), col("year"), col("score"))
                .groupBy(col("subject"))
                .pivot(col("year"))
                .agg(
                        round(avg(col("score")), 2).as("average"),
                        round(stddev(col("score")), 2).as("stddev")
                )
                .na().fill(0);

        results.show();
    }
}
