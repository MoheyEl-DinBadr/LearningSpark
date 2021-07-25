package com.mohey;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.regex.Pattern;

public class MainDeskRead {
    public static Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");

    public static void main(String[] args) {
        //.setMaster(local[*]) is for making multithreaded locally
        //When deploying to a cluster remove .setMaster("local[*]")
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Runtime.getRuntime().addShutdownHook(new Thread(()->
            sc.close()
        ));
        //When deploying on aws cluster or outside the code you pass the textFile path outside the jar
        //s3n://vpp-spark-demos/input.txt
        JavaRDD<String> initialRDDD = sc.textFile("src/main/resources/subtitles/input.txt", 35);

        JavaPairRDD<Long, String> sortedRDDD = initialRDDD.
                map(object -> object.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase()).
                filter(v1 -> !v1.trim().isBlank()).
                flatMap(value -> Arrays.stream(value.split(" ")).iterator()).
                filter(v1 -> !v1.equals("") && !Util.isBoring(v1)).
                mapToPair(s -> Tuple2.apply(s,1L)).
                reduceByKey(Long::sum).
                mapToPair(tuple2 -> Tuple2.apply(tuple2._2(), tuple2._1())).
                sortByKey(false);

        System.out.println("sortedRDDD.getNumPartitions() = " + sortedRDDD.getNumPartitions());
        sortedRDDD.take(10).forEach(System.out::println);

//                forEach(stringIntegerTuple2 -> System.out.println("Key: " +
//                        stringIntegerTuple2._1() + ", Repetitions: " + stringIntegerTuple2._2()));


        while(true){
            //To to be able to open localhost:4040 to view workers status
        }


    }
    public static boolean isNumeric(String data) {
        if (data == null) {
            return false;
        }
        return pattern.matcher(data).matches();
    }
}
