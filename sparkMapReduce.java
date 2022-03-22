import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.nio.file.Paths;
import java.util.Arrays;

public class sparkMapReduce {

    static {
        String OS = System.getProperty("os.name").toLowerCase();

        if (OS.contains("win")) {
            System.setProperty("hadoop.home.dir", Paths.get("winutils").toAbsolutePath().toString());
        } else {
            System.setProperty("hadoop.home.dir", "/");
        }
    }
    public static void main(String[] args) {
        SparkConf config = new SparkConf().setMaster("local").setAppName("text_counter");
        JavaSparkContext sc = new JavaSparkContext(config);
        JavaRDD<String> textFile = sc.textFile("processedDB.txt");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        System.out.println(counts);
    }
}
