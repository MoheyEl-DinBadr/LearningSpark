package com.mohey;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author Mohey El-Din Badr
 * @date 7/25/21 11:15 PM
 * @email MoheyElDin.Badr@gmail.com
 */
public class MainSQL {
    public static void main(String[] args){
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().
                master("local[*]").appName("StartingSparkSQL").getOrCreate();


        Dataset<Row> dataset = sparkSession.read()
                .option("header", true).csv("src/main/resources/exams/students.csv");


        Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' ");


        sparkSession.close();
    }
}
