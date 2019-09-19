package jpt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SparkLogAnalysis {
    static LogReg logReg;
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws ParseException {
        Date startDate = dateFormat.parse(args[1]);
        Date endDate = dateFormat.parse(args[2]);

        //Creating Spark Context
        JavaSparkContext sContext = new JavaSparkContext(new SparkConf().setAppName("ApacheLog").setMaster("local"));

        // Loading input data
        JavaRDD<String> lines = sContext.textFile("input/apacheLog.txt");

        // Counting 401 errors
        JavaPairRDD<String, Integer> counts = lines
                .filter(f->logReg.readLine(f).getLogDate().after(fromDate)
                        &&logReg.getLogDate().before(toDate)
                        &&logReg.get(field.Response).equals("401"))
                .mapToPair(line -> new Tuple2<String, Integer>("401 errors:", 1))
                .reduceByKey((x, y) -> x + y);

        // Saving word count in a text file
        counts.saveAsTextFile(args[3]);

        // Counting Top IP
        JavaPairRDD<String, Integer> IPs = lines
                .mapToPair(line -> new Tuple2<String, Integer>(logReg.readLine(line).get(field.IP), 1))
                .reduceByKey((x, y) -> x + y)
                .filter(f->f._2>=20);

       
        IPs.saveAsTextFile(args[3]);

        sContext.close();


    }
}