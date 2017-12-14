package spark.sample;


import java.util.Arrays;
import static spark.sample.Sample.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
			
			//키의 변경없이 value만 변경 
			JavaPairRDD<String, String> mapValues = pairs.mapValues(v->"--"+v);
			debug("mapValues", mapValues);
			
		}
	}
}
