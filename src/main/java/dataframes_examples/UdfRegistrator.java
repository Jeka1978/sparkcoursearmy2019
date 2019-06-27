package dataframes_examples;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;

/**
 * @author Evgeny Borisov
 */
@Component
public class UdfRegistrator implements ApplicationListener<ContextRefreshedEvent> {
    @Autowired

    private SQLContext sqlContext;


    @Override
    public void onApplicationEvent(ContextRefreshedEvent context) {
        Collection<Object> beans = context.getApplicationContext().getBeansWithAnnotation(RegisterUdf.class).values();
        for (Object bean : beans) {
            sqlContext.udf().register(bean.getClass().getName(), (UDF2<?, ?, ?>) bean,DataTypes.LongType);
        }
    }
}





