package spark.examples.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * This class uses ViewReportsSimulatorForBothViewingFigures as source of data
 */
public class ViewingFiguresStructuredVersion {

    public static void main(String[] args) throws InterruptedException, StreamingQueryException {

        // The following property has to be set only on windows environments
        System.setProperty("hadoop.home.dir", "d:/dev/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        /*
        To remove WARN logs like the following (caused when the code is not running in a cluster):
        20/02/19 16:33:54 WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.
        20/02/19 16:33:54 WARN BlockManager: Block input-0-1582126434400 replicated to only 0 peer(s) instead of 1 peers
        Use the following command:
         */
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        // Initializing:
        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("ViewingFiguresStructuredVersion")
                .getOrCreate();

        // Fix partitions number to increase performance and lower idle partitions
        session.conf().set("spark.sql.shuffle.partitions", 10);

        // Creating input stream:
        Dataset<Row> dataSet = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        dataSet.createOrReplaceTempView("viewing_figures");

        // Consuming the stream with the SparkSQL API:
        Dataset<Row> results = session.sql(
                "select window, cast(value as string) as course_name, sum(5) as seconds_watched " +
                        "from viewing_figures " +
                        "group by window(timestamp, '2 minutes'), course_name"
        );

        // Creating and starting query to write data to console sink:
        StreamingQuery query = results
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                .option("truncate", false)
                .option("numRows", 50)
                .start();

        // Instruction to make the code run forever:
        query.awaitTermination();
    }
}
