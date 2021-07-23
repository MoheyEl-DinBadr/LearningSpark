package com.mohey;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MainPairRDDs {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        //.setMaster(local[*]) is for making multithreaded locally
        //When deploying to a cluster remove .setMaster("local[*]")
        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> sparkContext.close()));

        JavaRDD<String> mainRDD = sparkContext.parallelize(inputData);

        JavaPairRDD<String, String> keyValPair = mainRDD.mapToPair(s -> {
            String[] keyValuePair = s.split(":");
            return Tuple2.apply(keyValuePair[0], keyValuePair[1].trim());
        });
        keyValPair.collect().forEach(tuple2 -> {
            System.out.println("tuple2._1() = " + tuple2._1());
            System.out.println("tuple2._2() = " + tuple2._2());
        });

        System.out.println();

        JavaPairRDD<String, Iterable<String>> keyValuesPair = keyValPair.groupByKey();
        keyValuesPair.collect().forEach(stringIterableTuple2 -> {
            System.out.println("Key: " + stringIterableTuple2._1());
            System.out.println("Values: ");
            stringIterableTuple2._2().forEach(System.out::println);
            System.out.println();
        });

        //To get the count for each key
        JavaPairRDD<String, Long> count = mainRDD.mapToPair(s -> {
            String[] keyValuePair = s.split(":");
            return Tuple2.apply(keyValuePair[0], 1L);
        }).reduceByKey((v1, v2) -> v1 + v2);

        count.collect().forEach(stringLongTuple2 -> System.out.println("Key: " +
                stringLongTuple2._1() + ", Value: " + stringLongTuple2._2()));

        System.out.println();
        //Using FlatMap
        JavaRDD<String> flattenData = mainRDD.flatMap(s ->
                Arrays.stream(s.split(" ")).iterator());
        flattenData.collect().forEach(System.out::println);
        while(true){

        }
    }
}
