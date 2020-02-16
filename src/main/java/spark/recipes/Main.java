package spark.recipes;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

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
		
		Dataset<Row> dataSet = session.read().option("header", true).csv("src/main/resources/biglog.txt");

        SimpleDateFormat monthFormat = new SimpleDateFormat("MMMM", Locale.ENGLISH);
        session.udf().register("getMonthNumber", (String month) -> {
            Date date = monthFormat.parse(month);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            return calendar.get(Calendar.MONTH);
        }, DataTypes.IntegerType);

        dataSet.createOrReplaceTempView("logs");

        Dataset<Row> results = session.sql(
                "select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                        "from logs group by level, month order by getMonthNumber(month), level"
        );

        results.show(100);
	}

}
