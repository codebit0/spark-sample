package spark.sample;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WorkCount {

	public static void main(String[] args) {
		try(JavaSparkContext sc = SampleConfig.context()){
//			JavaRDD<String> textFile = sc.textFile("hdfs://...");
			//JavaRDD<String> textFile = sc.textFile("http://archive.ics.uci.edu/ml/machine-learning-databases/00398/dataset-CalheirosMoroRita-2017.csv");
			
			//다운로드 파일 JavaPairRDD
			String csv = scala.io.Source.fromURL("http://archive.ics.uci.edu/ml/machine-learning-databases/00398/dataset-CalheirosMoroRita-2017.csv", scala.io.Codec.ISO8859()).mkString();
			List<String> lists = Arrays.asList(csv.split("\\n"));
			// 1. 가장 쉽게 RDD를 만드는 방식 
			// 하지만 모든 데이터세트를 메모리에 넣으므로 테스팅 목적으로 사용 
			// -더 일반적인 방법은 외부 스토리지에서 데이터를 불러오는 방법 
			JavaRDD<String> textFile = sc.parallelize(lists);
			JavaPairRDD<String, Integer> counts = textFile
			    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
			    .mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String word) throws Exception {
						return new Tuple2<>(word, 1);
					}
				})
			    .reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer a, Integer b) throws Exception {
						return a + b;
					}
				});
			List<Tuple2<String,Integer>> collect = counts.collect();
			System.out.println(collect);
			//파티션 단위로 저장
			//counts.saveAsTextFile("/Users/bootcode/Data/spark");
			//하나의 파일로 합한 후 저장 
			counts.coalesce(1).saveAsTextFile("/Users/bootcode/Data/spark");
		}
	}
}
