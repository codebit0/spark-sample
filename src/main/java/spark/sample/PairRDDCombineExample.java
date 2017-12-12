package spark.sample;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRDDCombineExample {
	public static void main(String[] args) {
		try(JavaSparkContext sc = SampleConfig.context()){
			
			//다운로드 파일 JavaPairRDD
			String csv = scala.io.Source.fromURL("http://archive.ics.uci.edu/ml/machine-learning-databases/00398/dataset-CalheirosMoroRita-2017.csv", scala.io.Codec.ISO8859()).mkString();
			JavaRDD<String> lines = sc.parallelize(Arrays.asList(csv.split("\\n")));
			JavaPairRDD<String,Integer> nums = lines.mapToPair(s-> new Tuple2<String, Integer>(s.split(" ")[0], 1));
			
		}
	}
}
