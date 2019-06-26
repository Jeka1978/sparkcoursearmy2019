package songs_analyzer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

/**
 * @author Evgeny Borisov
 */
@Configuration
@ComponentScan
public class Conf {


    @Bean
    public JavaSparkContext sc(@Autowired SparkConf sparkConf){
        return new JavaSparkContext(sparkConf);
    }


    public static void main(String[] args) {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext(Conf.class);
    }
















}
