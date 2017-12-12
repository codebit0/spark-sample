package spark.sample;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class BasicRddOperation {

	public static void main(String[] args) {
		try(JavaSparkContext sc = SampleConfig.context()){
			
			//다운로드 파일 JavaPairRDD
			String csv = scala.io.Source.fromURL("http://archive.ics.uci.edu/ml/machine-learning-databases/00398/dataset-CalheirosMoroRita-2017.csv", scala.io.Codec.ISO8859()).mkString();
			JavaRDD<String> lines = sc.parallelize(Arrays.asList(csv.split("\\n")));
			JavaRDD<String> filterdLines = lines.filter(line->line.startsWith("D"));
	
			//구문상으로는 되지만 serializable 이 선언안되어 있다면 오류가 발생함 
			lines.foreach(System.out::println);
			
//			lines.foreach(new VoidFunction<String>(){
//				@Override
//				public void call(String arg0) throws Exception {
//					System.out.println(arg0);
//				}
//			});
			//최종적으로 이런 형태로 변환 가
			//lines.foreach((s)->System.out.println(s));
			//또는 scala 도움을 받아서 
			filterdLines.foreach(scala.Console::println);
			
//			JavaPairRDD<String,Integer> nums = lines.mapToPair(s-> new Tuple2<String, Integer>(s.split(" ")[0], 1));
			
		}
	}
}
