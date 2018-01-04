package spark.sample;


import java.util.Arrays;
import java.util.function.Function;

import static spark.sample.Sample.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;


public class PairRDDTransformOperation {

	public static void main(String[] args) {
		try(JavaSparkContext sc = context()){
			
			JavaRDD<String> rdd = sc.textFile("/usr/local/opt/apache-spark/README.md");
			//tuple2 list type
			JavaRDD<Tuple2<String, String>> tupleMap = rdd.map(l->new Tuple2<String,String>(l.trim().split(" ")[0].toLowerCase(), l));
			debug("tupleMap", tupleMap);
			
			//pair map type  
			JavaPairRDD<String, String> pairs = rdd.mapToPair(l->new Tuple2<String,String>(l.trim().split(" ")[0].toLowerCase(), l));
			debug("pairs", pairs);
			
			//key list 
			JavaRDD<String> keys = pairs.keys();
			debug("keys", keys);
			
			//value list
			JavaRDD<String> values = pairs.values();
			debug("values", values);
			
			//key로 정렬된 pair리스트 
			JavaPairRDD<String, String> sortByKey = pairs.sortByKey();
			debug("sortByKey", sortByKey);
			
			// 동일 key에 대해 값을 합하여 반환 
			JavaPairRDD<String, String> keyJoinValue = pairs.reduceByKey((v1, v2)-> v1 + " :: "+ v2);
			debug("keyJoinValue", keyJoinValue);
			
			//동일 키에 대한 값들을 그룹화 
			JavaPairRDD<String,Iterable<String>> groupByKey = pairs.groupByKey();
			debug("groupByKey", groupByKey);
			
			//키의 변경없이 value만 변경 - value에 -- 를 덧붙임 
			JavaPairRDD<String, String> mapValues = pairs.mapValues(v->"--"+v);
			debug("mapValues", mapValues);
			
			//------- 
			//pair rdd -> pair rdd  지만 flat하게 변환 
			JavaPairRDD<String,String> flatMapValues = groupByKey.flatMapValues(l->l);
			debug("flatMapValues", flatMapValues);
			
			//pair rdd -> rdd 로 변환 
			JavaRDD<String> flatMap = groupByKey.flatMap(t -> t._2().iterator());
			debug("flatMap", flatMap);

			//filter
			JavaPairRDD<String,String> filter = pairs.filter(t->t._1.contains("c"));
			debug("filter", filter);
			
			Function<String, Tuple2<String, Integer>> createAcc = new Function<String, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> apply(String t) {
					
					return new Tuple2<String, Integer>(t, 0);
				}
			};
			
			Function2<Tuple2<String, Integer>, Integer, Tuple2<String, Integer>> addAndCount = new Function2<Tuple2<String, Integer>, Integer, Tuple2<String, Integer>>() {

				@Override
				public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Integer v2) throws Exception {
					// TODO Auto-generated method stub
					Tuple2 r = new Tuple2(v1._1, v1._2 +1);
					return r;
				}
			};
			
			Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> combine = new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

				@Override
				public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2)
						throws Exception {
					return new Tuple2(v1._1, v1._2 + v2._2);
				}
			};
			
			//flatMapValues.combineByKey(createAcc, addAndCount, combine);
			 
		}
	}
}
