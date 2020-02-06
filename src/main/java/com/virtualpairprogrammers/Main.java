package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Main {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "D:/cursos/spark/Practicals/winutils-extra/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession session = SparkSession.builder()
				.appName("learningSparkSQL")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = session.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
//		Dataset<Row> modernArt = dataset.filter("subject = 'Modern Art' AND year >=  2007");
		
//		Dataset<Row> modernArt = dataset.filter(row -> {
//			return row.getAs("subject").equals("Modern Art") && (Integer.valueOf(row.getAs("year")) >= 2007);
//		});

		Dataset<Row> modernArt = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
		
		modernArt.show();
		
		long rows = dataset.count();
		System.out.println(rows);
		
	}

}
