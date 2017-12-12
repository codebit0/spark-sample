package spark.sample;


import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;


public class TransformOperation {

	public static void main(String[] args) {
		try(JavaSparkContext sc = SampleConfig.context()){
			
			JavaRDD<String> rdd = sc.textFile("/usr/local/opt/apache-spark/README.md");
			//1. filter : filter 조건이 true인 데이터만으로 이루어진 RDD 반환 
			JavaRDD<String> filter1 = rdd.filter(line->line.contains("spark"));
			JavaRDD<String> filter2 = rdd.filter(line->line.contains("run"));
			System.out.println("-----------  Spark filter");
			filter1.foreach((s)->System.out.println(s));
			System.out.println("-----------  Run filter");
			filter2.foreach((s)->System.out.println(s));
			
			//2. union :다른 RDD를 합한 RDD 
			JavaRDD<String> union = filter1.union(filter2);
			System.out.println("-----------  union");
			union.foreach((s)->System.out.println(s));
			
			JavaRDD<String[]> map = union.map((l)->{ 
				return l.split(" ");
			});
			System.out.println("-----------  map");
			map.foreach((s)->System.out.println(s));
			
			System.out.println("-----------  flatmap");
			JavaRDD<String> flatMap = union.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
			flatMap.foreach((s)->System.out.println(s));
		}
	}
}
