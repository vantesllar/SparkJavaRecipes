package com.virtualpairprogrammers;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Find10MostFrequentWords {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "D:/cursos/spark/Practicals/winutils-extra/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		/*
		 * Find 10 most frequent words in input
		 */
		JavaRDD<String> initialRdd = context.textFile("src/main/resources/subtitles/input-spring.txt");
		initialRdd
			.flatMap(line -> Arrays.asList(line.replaceAll("[^a-zA-Z ]", "").toLowerCase().split(" ")).iterator())
			.filter(word -> Util.isNotBoring(word) && !word.isEmpty())
			.mapToPair(word -> new Tuple2<String, Integer>(word, 1))
			.reduceByKey(Integer::sum)
			.takeOrdered(10, SerializableTupleComparator.INSTANCE)
			.forEach(tuple -> System.out.println(tuple._1 + " appears " + tuple._2 + " times"));
			
		context.close();
	}
	
	static class SerializableTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
		private static final long serialVersionUID = 37342357384203666L;
		final static SerializableTupleComparator INSTANCE = new SerializableTupleComparator();
		
		// The comparison is performed on the key's frequency assuming that the second field of Tuple2 is a count or frequency
		public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
			return t2._2.compareTo(t1._2);	// Descending
		}
	}
}
