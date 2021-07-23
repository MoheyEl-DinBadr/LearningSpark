package com.mohey;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
        double sum = originalIntegers.reduce((v1, v2) -> v1 + v2);
        System.out.println("sum = " + sum);
        System.out.println();

        JavaRDD<Double> sqrtRDD = originalIntegers.map(aValue -> Math.sqrt(aValue));
        sqrtRDD.collect().forEach(System.out::println);

        JavaPairRDD<Integer, Double> map =
                originalIntegers.mapToPair(integer -> Tuple2.apply(integer, Math.sqrt(integer)));

        System.out.println();

        map.collect().forEach(integerDoubleTuple2 -> {
            System.out.println("integerDoubleTuple2._1() = " + integerDoubleTuple2._1());
            System.out.println("integerDoubleTuple2._2() = " + integerDoubleTuple2._2());
        });

        sc.close();

    }
}
