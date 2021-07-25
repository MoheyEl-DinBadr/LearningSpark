package com.mohey;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * @author Mohey El-Din Badr
 * @date 7/24/21 2:03 AM
 * @email MoheyElDin.Badr@gmail.com
 */
public class TestJoins {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("startingSparkJoins").setMaster("local[6]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, Integer>> vistsRaw = new ArrayList<>();
        vistsRaw.add(new Tuple2<>(4,18));
        vistsRaw.add(new Tuple2<>(6,4));
        vistsRaw.add(new Tuple2<>(10,9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(Tuple2.apply(1, "John"));
        usersRaw.add(Tuple2.apply(2, "Bob"));
        usersRaw.add(Tuple2.apply(3, "Alan"));
        usersRaw.add(Tuple2.apply(4, "Doris"));
        usersRaw.add(Tuple2.apply(5, "Marybelle"));
        usersRaw.add(Tuple2.apply(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(vistsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> output = visits.join(users);
        output.take(10).forEach(System.out::println);

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> outputLeftJoin = visits.leftOuterJoin(users);
        outputLeftJoin.take(10).forEach(element-> System.out.println(element._2()._2().orElse("Not Registered")));

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> outputRightJoin = visits.rightOuterJoin(users);
        outputRightJoin.take(10).forEach(element -> System.out.println(element._2()._1().orElse(-1) + ", Data: " + element._2()._2()));

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> crossJoin = visits.cartesian(users);
        crossJoin.take(10).forEach(System.out::println);


        sc.close();
    }
}
