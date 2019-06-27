package dataframes_examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
public class Main {
    private static final String SALARY = "salary";
    private static final String AGE = "age";
    private static final String KEYWORDS = "keywords";
    private static final String KEYWORD = "keyword";
    public static final String AMOUNT = "amount";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("linked in");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.read().json("data/linkedIn/*");
        dataFrame.show();
        dataFrame.printSchema();
        Arrays.stream(dataFrame.schema().fields())
                .forEach(f -> System.out.println(f.dataType()));


        Column coefficient = when(col(AGE).leq(30), (5)).otherwise(10);

        DataFrame salaryDf = dataFrame.withColumn(SALARY,
                col(AGE).multiply(size(col(KEYWORDS))
                        .multiply(coefficient)

                ));
        salaryDf.show();


        Row row = salaryDf.withColumn(KEYWORD, explode(col(KEYWORDS)))
                .select(KEYWORD)
//                .groupBy(KEYWORD).count().as("amount")
                .groupBy(KEYWORD).agg(count(col(KEYWORD)).as(AMOUNT))
                .orderBy(col(AMOUNT).desc())
                .head();


        String mostPopular = row.getAs(KEYWORD);

        System.out.println("mostPopular = " + mostPopular);

        salaryDf.where(col(SALARY).leq(1200))
                .filter(array_contains(col(KEYWORDS),mostPopular))
                .show();












    }
}













