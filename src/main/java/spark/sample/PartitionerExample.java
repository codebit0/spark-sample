package spark.sample;

import static spark.sample.Sample.*;
//import static spark.sample.Sample.context;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class PartitionerExample {

	private static final Pattern SPACES = Pattern.compile("\\s+");

	public static void main(String[] args) {
		try (JavaSparkContext sc = context()) {

//			SparkSession session = SparkSession.builder().appName("JavaPageRank").getOrCreate();

			JavaRDD<String> lines = sc
					.textFile("/usr/local/opt/apache-spark/libexec/examples/src/main/resources/urldata.txt");
			debug("lines", lines);
			Optional<Partitioner> partitioner = lines.partitioner();
			System.out.println(partitioner);

			int numPartitions = lines.getNumPartitions();
			System.out.println(numPartitions);

			JavaPairRDD<String, Iterable<String>> groupByKey = lines.mapToPair(s -> {
				String[] parts = SPACES.split(s);
				return new Tuple2<>(parts[0], parts[1]);
			}).partitionBy(new HashPartitioner(10)).distinct().groupByKey();
			debug("groupByKey", groupByKey);

			// 파티셔닝은 그룹핑 join등 키 연산 성능향상에 도움을 줌
			// 파티셔닝 자체는 transform명령이므로 꼭 persist 나 cache 해야 이후 재 파티셔닝 하지 않음
			System.out.println(groupByKey.partitioner());
			System.out.println(groupByKey.getNumPartitions());

			JavaPairRDD<String, Iterable<String>> links = groupByKey.cache();
			// 키의 변경 없이 value만 1.0으로 초기화
			JavaPairRDD<String, Double> ranks = links.mapValues(u -> 1.0);
			debug("ranks", ranks);

			//10회 수행 
			for (int current = 0; current < 2; current++) {
				// 다른 url 과 rank 계산 
				JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(s -> {
					int urlCount = Iterables.size(s._1());
					List<Tuple2<String, Double>> results = new ArrayList<>();
					for (String n : s._1) {
						results.add(new Tuple2<>(n, s._2() / urlCount));
					}
					return results.iterator();
				});

				// rank 반영 
				ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(sum -> 0.15 + sum * 0.85);
			}

			// Collects all URL ranks and dump them to console.
			List<Tuple2<String, Double>> output = ranks.collect();
			for (Tuple2<?, ?> tuple : output) {
				System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
			}
//			https://github.com/abbas-taher/pagerank-example-spark2.0-deep-dive
		}
	}
}
