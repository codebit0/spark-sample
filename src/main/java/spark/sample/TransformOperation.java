package spark.sample;


import java.util.Arrays;
import static spark.sample.Sample.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class TransformOperation {

	public static void main(String[] args) {
		try(JavaSparkContext sc = context()){
			
			JavaRDD<String> rdd = sc.textFile("/usr/local/opt/apache-spark/README.md");
			//1. filter : filter 조건이 true인 데이터만으로 이루어진 RDD 반환 
			JavaRDD<String> filter1 = rdd.filter(line->line.contains("spark"));
			JavaRDD<String> filter2 = rdd.filter(line->line.contains("run"));
			
			debug("spark filter", filter1);
			debug("Run filter", filter2);
			
			//2. union :다른 RDD를 합한 RDD 
			JavaRDD<String> union = filter1.union(filter2);
			debug("union", union);
			
			JavaRDD<String[]> map = union.map((l)->{ 
				return l.split(" ");
			});
			debug("map", map);
			
			JavaRDD<String> flatMap = union.flatMap(line -> Arrays.asList(line.split(" ")).stream().filter(w->!w.isEmpty()).iterator());
			debug("flatmap", flatMap);
			
			//withReplacement true 복원추출 , false 비복원추출 - 복원추출 : 한번 선택한 샘플도 다시 뽑을 수 있음 
			//seed random seed 
			JavaRDD<String> sample = flatMap.sample(true, 0.5);
			debug("sample", sample);
			
			//중복제거
			JavaRDD<String> distinct = sample.distinct();
			debug("distinct", distinct);
		}
	}
}
