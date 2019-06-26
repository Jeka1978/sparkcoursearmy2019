package songs_analyzer;

import org.apache.spark.api.java.JavaRDD;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
@Service
public class TopXService {

    public List<String> topX(JavaRDD<String> lines, int x) {
        return lines.map(String::toLowerCase)
                .flatMap(WordsUtil::getWords)
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .values()
                .take(x);
    }


}
