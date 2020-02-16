package spark.recipes;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * In this example we analyse the popularity of different courses, based on the number of chapters per course,
 * the number of chapters seen by users and the number of chapters seen per course,
 * using the Spark's Java API, i.e., the RDDs and its methods.
 */
public class AnalysingCoursePopularity {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

        // The following property has to be set only on windows environments
        System.setProperty("hadoop.home.dir", "D:/cursos/spark/Practicals/winutils-extra/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;

		// Course chapters - (chapterId, courseId)
		JavaPairRDD<Integer, Integer> chaptersCourses = setUpChapterDataRdd(sc, testMode);
		// Chapter views - (userId, chapterId)
		JavaPairRDD<Integer, Integer> chaptersSeenByUser = setUpViewDataRdd(sc, testMode);
		// Course data - (courseId, title)
		JavaPairRDD<Integer, String> coursesTitles = setUpTitlesDataRdd(sc, testMode);

		// 1st: Find how many chapters has each course
		// (courseId, totalChaptersPerCourse)
		JavaPairRDD<Integer, Integer> chaptersPerCourse = chaptersCourses
				.mapToPair(row -> new Tuple2<>(row._2, 1)) // (chapterId, courseId) -> (courseId, 1) preparing data to reduce
				.reduceByKey(Integer::sum); // (courseId, totalChaptersPerCourse)

		// 2nd: Find How many chapters per course have been watched, on an user individual basis
		// (courseId, amountOfChaptersWatchedPerCourse)
		JavaPairRDD<Integer, Integer> chaptersWatchedPerCourse = chaptersSeenByUser
				.distinct() // remove duplicates (we don't want to count the same user watching the same chapter several times)
				.mapToPair(row -> new Tuple2<>(row._2, row._1)) // (userId, chapterId) -> (chapterId, userId)
				.join(chaptersCourses) // (chapterId, (userId, courseId))
				.mapToPair(row -> new Tuple2<>(row._2, 1)) // (chapterId, (userId, courseId)) -> ((userId, courseId), 1) preparing data to reduce
				.reduceByKey(Integer::sum) // ((userId, courseId), amountOfChaptersWatchedPerCoursePerUser)
				.mapToPair(row -> new Tuple2<>(row._1._2, row._2)); // ((userId, courseId), amountOfChaptersWatchedByUserForCourse) -> (courseId, amountOfChaptersWatchedPerCoursePerUser)

		// 3rd: Join data sets and compute score
		// target - (courseId, score)
		// We need to know, per course and user, the percentage of chapters the same user has seen, a table in the form of
		// | courseId | totalChaptersPerCourse | amountOfChaptersWatchedPerCoursePerUser |
		// where we should have multiples entries for the same key, each representing a different user (amountOfChaptersWatchedPerCourse is per user)
		JavaPairRDD<Integer, Integer> scores = chaptersPerCourse.join(chaptersWatchedPerCourse) // (courseId, (totalChaptersPerCourse, amountOfChaptersWatchedPerCoursePerUser))
				.mapValues(value -> {
					double percentage = 100.0 * value._2 / value._1;
					int score;
					if (percentage > 90.0) {
						score = 10;
					} else if(percentage > 50.0) {
						score = 4;
					} else if(percentage > 25.0) {
						score = 2;
					} else {
						score = 0;
					}
					return score;
				})
				.reduceByKey(Integer::sum); // (courseId, score)

		// 4th: join with the data set that has the course names
		JavaPairRDD<String, Integer> scoresPerCourse = scores.join(coursesTitles) // (courseId, (score, title))
				.mapToPair(row -> new Tuple2<>(row._2._1, row._2._2))
				.sortByKey(false) // descending by score
				.mapToPair(row -> new Tuple2<>(row._2, row._1)); // (title, score))

		scoresPerCourse.collect().forEach(System.out::println);
		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();

		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (courseId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, courseId)
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	}).cache();
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
