import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Calendar;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by Michael on 11/5/15.
 */
public final class Histogram_Movies {
    private final static float division = 0.5f;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Histogram-Movies").setMaster("local[1]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        java.sql.Timestamp startTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
        long startTime = System.nanoTime();
        JavaRDD<String> lines = ctx.textFile("../Classification/file1", 1);

        JavaPairRDD<Float, Integer> averageRating = lines.mapToPair(new PairFunction<String, Float, Integer>() {
            @Override
            public Tuple2<Float, Integer> call(String s) throws Exception {
                int rating, movieIndex, reviewIndex;
                int totalReviews = 0, sumRatings = 0;
                float avgReview = 0.0f, absReview, fraction, outValue = 0.0f;
                String reviews = new String();
                String line = new String();
                String tok = new String();
                String ratingStr = new String();

                movieIndex = s.indexOf(":");
                if(movieIndex > 0) {
                    reviews = s.substring(movieIndex + 1);
                    StringTokenizer token = new StringTokenizer(reviews, ",");
                    while (token.hasMoreTokens()) {
                        tok = token.nextToken();
                        reviewIndex = tok.indexOf("_");
                        ratingStr = tok.substring(reviewIndex + 1);
                        rating = Integer.parseInt(ratingStr);
                        sumRatings += rating;
                        totalReviews ++;
                    }
                    avgReview = (float) sumRatings / (float) totalReviews;
                    absReview = (float) Math.floor((double)avgReview);

                    fraction = avgReview - absReview;
                    int limitInt = Math.round(1.0f/division);

                    for (int i = 1; i <= limitInt; i++){
                        //introduce error here. With =, we will have
                        //(0.5, 1] => 1, (1, 1.5] => 1.5, (1.5, 2] => 2, etc
                        //Without =, which is the same version as in Hadoop MapReduce, we will have
                        //[0.5, 1) => 1, [1, 1.5) => 1.5, [1.5, 2) => 2, etc
                        //The result will be different.
                        if(fraction < (division * i) ){
                            outValue = absReview + division * i;
                            break;
                        }
                    }
//                    if (avgReview < 0.5) {
//                        outValue = 0.0f;
//                    } else if (avgReview < 1) {
//                        outValue = 1.0f;
//                    } else if (avgReview < 1.5) {
//                        outValue = 1.5f;
//                    } else if (avgReview < 2) {
//                        outValue = 2.0f;
//                    } else if (avgReview < 2.5) {
//                        outValue = 2.5f;
//                    } else if (avgReview < 3.0) {
//                        outValue = 3.0f;
//                    } else if (avgReview < 3.5) {
//                        outValue = 3.5f;
//                    } else if (avgReview < 4.0) {
//                        outValue = 4.0f;
//                    } else if (avgReview < 4.5) {
//                        outValue = 4.5f;
//                    } else {
//                        outValue = 5.0f;
//                    }
                }
                Tuple2<Float, Integer> result = new Tuple2<Float, Integer>(outValue, 1);
                return result;
            }
        });

        JavaPairRDD<Float, Integer> counts = averageRating.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        long endTime = System.nanoTime();
        java.sql.Timestamp endTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

        System.out.println("This job started at " + startTimestamp);
        System.out.println("This job finished at: " + endTimestamp);
        System.out.println("The job took: " + (endTime - startTime)/1000000 + " milliseconds to finish");

        // To check the result
        List<Tuple2<Float, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        //

        ctx.stop();
    }


}
