package spark.sample;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;


public class NormalizerSample {
	public static void main(String[] args) {
		try(JavaSparkContext sc = Sample.context()){
			
			Vector vector1 = Vectors.dense(new double[]{-2.0, 5.0, 1.0});
			Vector vector2 = Vectors.dense(new double[]{2.0, 0.0, 1.0});
			List<Vector> vectors = new ArrayList<Vector>() {
				{
					add(vector1);
					add(vector2);
				}
			};
			
			JavaRDD<Vector> dataset = sc.parallelize(vectors);
			
			StandardScaler sscaler = new StandardScaler(true, true);
			StandardScalerModel model = sscaler.fit(JavaRDD.toRDD(dataset));
			RDD<Vector> result = model.transform(JavaRDD.toRDD(dataset));
			System.out.println(result);
			
			Vector[] collect = (Vector[]) result.collectPartitions();
			System.out.println(collect);
			
			//-0.7071 , 0.7071 , 0.0 , 0.7071 -0.7071 , 0.0
		}
	}
}
