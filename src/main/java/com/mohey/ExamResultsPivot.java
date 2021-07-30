package com.mohey;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

/**
 * @author Mohey El-Din Badr
 * @date 7/28/21 5:28 PM
 * @email MoheyElDin.Badr@gmail.com
 */
public class ExamResultsPivot {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                                                    .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        /*Column scoreInt = col("score").cast(DataTypes.IntegerType);

        dataset = dataset.groupBy("subject").agg(max(scoreInt).as("max_score"), min(scoreInt).as("mix_score"));

        dataset.show();*/

        dataset.groupBy("student_id","year", "quarter").pivot("subject")
                .agg(count("subject").as("count"),
                        round(avg("score"), 2).as("avg_score"))
                .na().fill(0).orderBy("student_id","year").show();

        dataset.groupBy("subject")
                .pivot("year")
                .agg(round(avg("score"), 2).as("average"),
                        round(stddev("score"), 2).as("stddev")
                )
                .show();
    }
}
