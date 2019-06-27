package dataframes_examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author Evgeny Borisov
 */
@Configuration
@ComponentScan
public class Conf {

    @Bean
    public JavaSparkContext sc(){
        return new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("taxi"));
    }

    @Bean
    public SQLContext sqlContext(){
        return new SQLContext(sc());
    }

    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Conf.class);
        context.getBean(LinkedInService.class).printAll();
    }
}





