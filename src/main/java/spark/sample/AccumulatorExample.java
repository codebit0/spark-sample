package spark.sample;

import static spark.sample.Sample.context;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.util.LongAccumulator;

public class AccumulatorExample {

	private static AtomicLong accum3 =  new AtomicLong(0);
	
	public static void main(String[] args) {
		try (JavaSparkContext sc = context()) {
			AtomicLong accum2 =  new AtomicLong(0);
			LongAccumulator accum = sc.sc().longAccumulator();
			sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
			System.out.println(accum.value());
			
			sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x ->{ accum2.addAndGet(x);});
			System.out.println(accum2);
			
			sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum3.addAndGet(x));
			
			AtomicLong accum4 =  new AtomicLong(0);
			Integer reduce = sc.parallelize(Arrays.asList(1, 2, 3, 4)).reduce(new Function2<Integer,Integer,Integer>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Integer call(Integer v1, Integer v2) throws Exception {
					accum4.getAndSet(v1+ v2);
					return v1+ v2;
				}
			});
			System.out.println(accum4);
			System.out.println(reduce);
			//accumilator는 장애 내결함성을 가지나 transform 에서는 중복 연산이 될 가능성이 높음 acc는 foreach 같은 action 쪽에 배치하여야 내결함성이 확보됨  
			
		}
	}
}
