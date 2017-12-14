package spark.sample;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

public class Sample {

	public static SparkConf conf() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.set("spark.driver.host", "127.0.0.1");
		sparkConf.setMaster("local[*]");
		sparkConf.setAppName("Hello Spark");
		return sparkConf;
	}

	public static JavaSparkContext context() {
		JavaSparkContext context = new JavaSparkContext(conf());
		context.setLogLevel("ERROR");
//		Logger.getLogger("org").setLevel(Level.OFF);
		return context;
	}
	
	public static void debug(String title, JavaRDD<?> rdd) {
		System.out.println(title+" ----------------------------");
		rdd.foreach((s)->System.out.println("\t\t "+s));
		System.out.println(" ----------------------------");
		System.out.println();
	}
	
	public static void debug(String title, JavaPairRDD<?, ?> rdd) {
		// TODO Auto-generated method stub
		System.out.println(title+" ----------------------------");
		rdd.foreach((s)->System.out.println("\t\t "+s));
		System.out.println(" ----------------------------");
		System.out.println();
	}
}
