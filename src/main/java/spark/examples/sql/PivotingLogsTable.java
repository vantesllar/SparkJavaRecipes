package spark.examples.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * In this example we get the amount of logs of each level per month,
 * using the groupBy, pivot and count Dataset (SparkSQL) methods.
 */
public class PivotingLogsTable {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

	    // The following property has to be set only on windows environments
		System.setProperty("hadoop.home.dir", "D:/cursos/spark/Practicals/winutils-extra/hadoop");

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession session = SparkSession.builder()
				.appName("learningSparkSQL")
				.master("local[*]")
//				.config("spark.sql.warehouse.dir", "file:///c:/tmp/") // Windows
                .config("spark.sql.warehouse.dir", "file:/Users/ivan/Desktop/tmp/") // macOS
				.getOrCreate();
		
		Dataset<Row> dataSet = session.read()
                .option("header", true)
                .csv("src/main/resources/biglog.txt");

		// If we know the columns we have, we can provide them as list to enforce the order we want when pivoting
        List<Object> columns = Arrays.asList("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December");

        Dataset<Row> results = dataSet
                .select(col("level"), date_format(col("datetime"), "MMMM").alias("month"))
                .groupBy(col("level"))
                .pivot("month", columns)
                .count()
                .na().fill(0);

        results.explain();

        results.show(100);
	}
}
