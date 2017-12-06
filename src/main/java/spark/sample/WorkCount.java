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
//			JavaRDD<String> textFile = sc.textFile("hdfs://...");
			//JavaRDD<String> textFile = sc.textFile("http://archive.ics.uci.edu/ml/machine-learning-databases/00398/dataset-CalheirosMoroRita-2017.csv");
			
			//다운로드 파일 JavaPairRDD
			String csv = scala.io.Source.fromURL("http://archive.ics.uci.edu/ml/machine-learning-databases/00398/dataset-CalheirosMoroRita-2017.csv", scala.io.Codec.ISO8859()).mkString();
			List<String> lists = Arrays.asList(csv.split("\\n"));
			JavaRDD<String> textFile = sc.parallelize(lists);
			JavaPairRDD<String, Integer> counts = textFile
			    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
			    .mapToPair(word -> new Tuple2<>(word, 1))
			    .reduceByKey((a, b) -> a + b);
			List<Tuple2<String,Integer>> collect = counts.collect();
			System.out.println(collect);
		}
	}
}
