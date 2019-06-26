package songs_analyzer;

import org.apache.spark.api.java.JavaRDD;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
@Service
public class TopXService {

    public List<String> topX(JavaRDD<String> lines, int x) {
        return null;
    }







}
