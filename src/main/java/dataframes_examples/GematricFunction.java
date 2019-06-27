package dataframes_examples;

import org.apache.spark.sql.api.java.UDF2;

/**
 * @author Evgeny Borisov
 */
@RegisterUdf
public class GematricFunction implements UDF2<String,Long,Long> {
    @Override
    public Long call(String name, Long age) throws Exception {
      if(name.toLowerCase().contains("stark")){
          return 0L;
      }
        return 120 - age - name.length();
    }
}
