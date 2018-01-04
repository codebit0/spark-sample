package spark.sample;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static spark.sample.Sample.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class BasicActionOperation {

	public static void main(String[] args) {
		try (JavaSparkContext sc = context()) {

			JavaRDD<String> rdd = sc.textFile("/usr/local/opt/apache-spark/README.md");
			// 1. filter : filter 조건이 true인 데이터만으로 이루어진 RDD 반환
			// JavaRDD<String> filter1 = rdd.filter(line->line.contains("spark"));
			JavaRDD<String> filter1 = rdd.filter(line -> line.contains("run")).map(s -> s.trim().split(" ", 2)[0]);

			long count = filter1.count();
			debug("count", count);

			List<String> collect = filter1.collect();
			debug("collect", collect);

			List<String> top = filter1.top(10);
			debug("top", top);

			List<String> take = filter1.take(10);
			debug("take", take);

			List<String> takeOrdered = filter1.takeOrdered(10);
			debug("takeOrdered", takeOrdered);

			takeOrdered = filter1.takeOrdered(10, new MyComparator());
			debug("takeOrdered - Comparator", takeOrdered);

			List<String> takeSample = filter1.takeSample(false, 3);
			debug("takeSample", takeSample);
			
			//같은 value 의 count 맵 
			Map<String, Long> countByValue = filter1.countByValue();
			debug("countByValue", countByValue);
			
			String reduce = filter1.reduce(new Function2<String,String,String>(){
				private static final long serialVersionUID = 1L;
				
				@Override
				public String call(String v1, String v2) throws Exception {
					return v1 + " " + v2 ;
				}
			});
			
			debug("reduce", reduce);
			
			//reduce zeroValue는 각 파티션에서 누적해야 될 값의 시작값
			String fold = filter1.fold("zeroValue", new Function2<String,String,String>(){
				private static final long serialVersionUID = 1L;
				
				@Override
				public String call(String v1, String v2) throws Exception {
					return v1 + " " + v2 ;
				}
			});
			debug("fold", fold);
			
			//reduce 와 유사하나 다른 타입을 리턴 
			Integer aggregate = filter1.aggregate(0, 
				new Function2<Integer, String, Integer>(){
					@Override
					public Integer call(Integer v1, String v2) throws Exception {
						System.out.println(v1+ " - "+v2 + " "+v2.length());
						return v1+ v2.length();
					}
				}, 
				new Function2<Integer, Integer, Integer>(){
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
				}
			);
			debug("aggregate", aggregate);
		}
	}
	
	public static class MyComparator implements Comparator<String>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(String o1, String o2) {
//			char charAt = o1.charAt(0);
//			char charAt2 = o2.charAt(0);
//			System.out.println(charAt+"-"+Character.codePointAt(o1, 0) + " : "+ charAt2+ "-" +Character.codePointAt(o2, 0));
			return o1.compareTo(o2);
		}
	}
}
