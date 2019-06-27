package dataframes_examples;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;

import static dataframes_examples.Const.*;
import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
@Service
public class LinkedInService {

    @Autowired
    private SQLContext sqlContext;


    public void printAll() {
        DataFrame dataFrame = sqlContext.read().json("data/linkedIn/*");

        DataFrame salaryDf = getSalaryDataFrame(dataFrame);
        salaryDf.show();

        String mostPopular = getMostPopular(salaryDf);

        System.out.println("mostPopular = " + mostPopular);

        salaryDf.where(col(SALARY).leq(1200))
                .filter(array_contains(col(KEYWORDS), mostPopular))
                .show();

        salaryDf.withColumn("age to live",
                callUDF(GematricFunction.class.getName(), col("name"),col(AGE))).show();

    }

    private String getMostPopular(DataFrame salaryDf) {
        Row row = salaryDf.withColumn(KEYWORD, explode(col(KEYWORDS)))
                .select(KEYWORD)
//                .groupBy(KEYWORD).count().as("amount")
                .groupBy(KEYWORD).agg(count(col(KEYWORD)).as(AMOUNT))
                .orderBy(col(AMOUNT).desc())
                .head();


        return row.getAs(KEYWORD);
    }

    private DataFrame getSalaryDataFrame(DataFrame dataFrame) {
        Column coefficient = when(col(AGE).leq(30), (5)).otherwise(10);
        return dataFrame.withColumn(SALARY,
                    col(AGE).multiply(size(col(KEYWORDS))
                            .multiply(coefficient)

                    ));
    }
}
