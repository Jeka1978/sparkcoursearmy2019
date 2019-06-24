package spark_demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        SparkConf conf = sparkConf.setMaster("local[*]").setAppName("demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("stam.txt");

        JavaRDD<String> rdd2 = rdd.map(String::toLowerCase);
        JavaRDD<String> rdd3 = rdd2.map(String::toUpperCase);
        rdd3.saveAsTextFile("");
        List<String> list = rdd3.collect();

    }
}












