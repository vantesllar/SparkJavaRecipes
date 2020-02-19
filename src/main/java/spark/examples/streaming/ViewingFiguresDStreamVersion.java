package spark.examples.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * This class uses ViewReportsSimulatorForBothViewingFigures as source of data
 */
public class ViewingFiguresDStreamVersion {

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
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(1));

        // Defining the Kafka consumer configuration:
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-streaming-group");
        kafkaParams.put("auto.offset.reset", "latest");
//        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("viewrecords");

        // Creating the input stream connected to Kafka:
        JavaInputDStream<ConsumerRecord<String, String>> inputStream = KafkaUtils.createDirectStream(
                context,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        // Windowed aggregation with interval
        JavaPairDStream<Long, String> results = inputStream.mapToPair(item -> new Tuple2<>(item.value(), 1L))
                .reduceByKeyAndWindow(Long::sum, Durations.minutes(60), Durations.minutes(1))
                .mapToPair(Tuple2::swap)
                .transformToPair(rdd -> rdd.sortByKey(false));

        results.print();

        context.start();
        context.awaitTermination();
    }
}
