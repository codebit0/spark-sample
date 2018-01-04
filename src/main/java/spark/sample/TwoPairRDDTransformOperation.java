package spark.sample;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static spark.sample.Sample.*;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class TwoPairRDDTransformOperation {

	public static void main(String[] args) {
		try (JavaSparkContext sc = context()) {

			List<Character> chars = IntStream.rangeClosed('a', 'z').mapToObj(c -> (char) c)
					.collect(Collectors.toList());
			List<Tuple2<Character, Integer>> list1 = chars.stream().limit(15)
					.map(c -> new Tuple2<Character, Integer>(c, Character.digit(c, 32))).collect(Collectors.toList());
			List<Tuple2<Character, Integer>> list2 = chars.stream().skip(10)
					.map(c -> new Tuple2<Character, Integer>(c, Character.digit(c, 24))).collect(Collectors.toList());

			JavaPairRDD<Character, Integer> pair1 = sc.parallelizePairs(list1);
			JavaPairRDD<Character, Integer> pair2 = sc.parallelizePairs(list2);

			// inner join
			JavaPairRDD<Character, Tuple2<Integer, Integer>> join = pair1.join(pair2);
			debug("join", join);

			// left out join
			JavaPairRDD<Character, Tuple2<Integer, Optional<Integer>>> leftOuterJoin = pair1.leftOuterJoin(pair2);
			debug("leftOuterJoin", leftOuterJoin.sortByKey());

			// right out join
			JavaPairRDD<Character, Tuple2<Optional<Integer>, Integer>> rightOuterJoin = pair1.rightOuterJoin(pair2);
			debug("rightOuterJoin", rightOuterJoin.sortByKey(true, 1));

			// ------------
			JavaPairRDD<Character, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroup = pair1.cogroup(pair2);
			debug("cogroup", cogroup);
			cogroup.map(x -> {
				Character key = x._1();
				Tuple2<Iterable<Integer>, Iterable<Integer>> group = x._2();
				group._1().forEach(System.out::println);
				group._2().forEach(System.out::println);
				return key;
			});

			// --- 기타
			// 파티션 갯수 변경 shuffle 여부
			JavaPairRDD<Character, Integer> coalesce = pair1.coalesce(2, true);
			// 파티션을 다시 shuffle 파티션 갯수 변경
			JavaPairRDD<Character, Integer> repartition = pair1.repartition(2);
			pair1.repartitionAndSortWithinPartitions(new Partitioner() {
				@Override
				public int getPartition(Object arg0) {
					return 0;
				}

				@Override
				public int numPartitions() {
					return 0;
				}
			});
			//영속성 처리  LRU 정책  
			pair1.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
			pair1.count();
			pair1.collect().stream().toArray();
			pair1.unpersist();
		}

	}
}
