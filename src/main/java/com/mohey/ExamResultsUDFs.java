package com.mohey;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

/**
 * @author Mohey El-Din Badr
 * @date 7/29/21 8:52 PM
 * @email MoheyElDin.Badr@gmail.com
 */
public class ExamResultsUDFs {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkLearning").getOrCreate();
        spark.udf().register("hasPassed", (String grade, String subject) ->{
                    if(subject.toLowerCase().equals("biology")){
                        if (grade.startsWith("A")) return true;
                        return false;
                    }
                    return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
                }
                ,
                DataTypes.BooleanType);

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        //dataset.withColumn("pass", lit(col("grade").equalTo("A+"))).show();
        dataset.withColumn("pass",
                callUDF("hasPassed", col("grade"), col("subject"))).show();
    }


}
