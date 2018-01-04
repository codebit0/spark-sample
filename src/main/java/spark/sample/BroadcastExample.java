package spark.sample;

import static spark.sample.Sample.context;

import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastExample {

	public static void main(String[] args) {
		try (JavaSparkContext sc = context()) {
			Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});
			int[] value = broadcastVar.value();
			Arrays.stream(value).forEach(System.out::println);
			
//			sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreachPartition(f->f.forEachRemaining(c->{
//					System.out.println(c + " " + broadcastVar.value()[0]);
//				}
//			));
//			new Function2<Integer, Iterator<Integer>,Iterator<Integer>>() {
//				
			//};
//			sc.parallelize(Arrays.asList(1, 2, 3, 4)).mapPartitionsWithIndex();
//		));
		}
	}
}
