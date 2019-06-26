package dataframes_examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("linked in");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);


        DataFrame dataFrame = sqlContext.read().json("data/linkedIn/*");

//        dataFrame.filter(col("age").leq(40))
//        dataFrame.filter(array_contains(col("keywords"),"spring"))
        dataFrame = dataFrame.withColumn("2", size(col("keywords"))).select("2");
//        dataFrame = dataFrame.withColumn("2", explode(col("keywords"))).select("2");

        dataFrame.show();
//        dataFrame.withColumn("number of technologies")







//
//        Row[] rows = dataFrame.take(5);
//        for (Row row : rows) {
//            String name = row.getAs("name");
//            System.out.println("name = " + name);
//        }


    }
}
