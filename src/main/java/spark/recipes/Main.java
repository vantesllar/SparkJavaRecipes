package spark.recipes;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Main {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

	    // The following property has to be set only on windows environments
		System.setProperty("hadoop.home.dir", "D:/cursos/spark/Practicals/winutils-extra/hadoop");

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession session = SparkSession.builder()
				.appName("learningSparkSQL")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = session.read().option("header", true).csv("src/main/resources/biglog.txt");

//        Dataset<Row> results = dataset
//                .select(col("level"),
//                        date_format(col("datetime"), "MMMM").alias("month"),
//                        date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType))
//                .groupBy(col("level"), col("month"), col("monthnum"))
//                .count()
//                .orderBy("monthnum")
//                .drop(col("monthnum"));

        String[] columns = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};

        Dataset<Row> results = dataset
                .select(col("level"),
                        date_format(col("datetime"), "MMMM").alias("month"),
                        date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType))
                .groupBy(col("level"))
                .pivot("month", Arrays.asList(columns))
                .count()
                .na().fill(0);

        results.show(100);
	}

}
