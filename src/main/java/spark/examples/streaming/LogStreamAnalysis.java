package spark.examples.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * This class uses LoggingServerForLogStreamAnalysis as source of data
 */
public class LogStreamAnalysis {

    public static void main(String[] args) throws InterruptedException {

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

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("LogStreamAnalysis");
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(2));

        // Creating a DStream connected to a socket of text
        JavaReceiverInputDStream<String> inputStream = context.socketTextStream("localhost", 8989);

        // Transform to JavaPairDStream so we can aggregate afterwards
        JavaPairDStream<String, Long> pairResults = inputStream.mapToPair(rawLogMessage -> new Tuple2<>(rawLogMessage.split(",")[0], 1L));

        // Aggregate counting per key
        pairResults.reduceByKeyAndWindow(Long::sum, Durations.minutes(1)).print();

        context.start();
        context.awaitTermination();
    }
}
