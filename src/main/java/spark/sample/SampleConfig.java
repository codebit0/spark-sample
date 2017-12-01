package spark.sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SampleConfig {

	public static SparkConf conf() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.set("spark.driver.host", "127.0.0.1");
		sparkConf.setMaster("local[*]");
		sparkConf.setAppName("Hello Spark");
		return sparkConf;
	}

	public static JavaSparkContext context() {
		JavaSparkContext context = new JavaSparkContext(conf());
		return context;
	}
}
