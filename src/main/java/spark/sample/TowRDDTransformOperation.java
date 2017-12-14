package spark.sample;


import java.util.Arrays;
import java.util.List;

import static spark.sample.Sample.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class TowRDDTransformOperation {

	public static void main(String[] args) {
		try(JavaSparkContext sc = context()){
			
			List<Integer> data1 = Arrays.asList(1, 2, 3, 4, 5);
			List<Integer> data2 = Arrays.asList(3, 4, 5, 6, 7);
			List<Integer> data3 = Arrays.asList(6, 7, 8, 9, 10);
			JavaRDD<Integer> d1 = sc.parallelize(data1);
			JavaRDD<Integer> d2 = sc.parallelize(data2);
			JavaRDD<Integer> d3 = sc.parallelize(data3);
			//합집합 
			JavaRDD<Integer> union = d1.union(d2);
			debug("union", union);
			
			//교집합 
			JavaRDD<Integer> intersection = union.intersection(d3);
			debug("intersection", intersection);
			
			//차집합 
			JavaRDD<Integer> subtract = union.subtract(d3);
			debug("subtract", subtract);
			
			//카테시안 곱 
			JavaPairRDD<Integer, Integer> cartesian = union.cartesian(d3);
			debug("cartesian", cartesian);
		}
	}
}
