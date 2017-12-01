package spark.sample;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WorkCount {

	public static void main(String[] args) {
		try(JavaSparkContext sc = SampleConfig.context()){
			JavaRDD<String> textFile = sc.textFile("hdfs://...");
			JavaPairRDD<String, Integer> counts = textFile
			    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
			    .mapToPair(word -> new Tuple2<>(word, 1))
			    .reduceByKey((a, b) -> a + b);
			List<Tuple2<String,Integer>> collect = counts.collect();
			System.out.println(collect);
		}
	}
}
