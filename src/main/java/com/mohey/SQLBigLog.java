package com.mohey;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.DateFormatSymbols;
import java.time.Month;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * @author Mohey El-Din Badr
 * @date 7/27/21 12:53 PM
 * @email MoheyElDin.Badr@gmail.com
 */
public class SQLBigLog {
    public static void main(String[] args) {
        SparkSession session = new SparkSession.Builder()
                .appName("LearningSparkSQL").master("local[*]").getOrCreate();

        Dataset<Row> logs = session.read().option("header", true).
                csv("src/main/resources/logs/biglog.txt");

        logs.createOrReplaceTempView("logging_table");
        //multipleGroupingsTest(session);
        /*logs = logs.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum"))
                .groupBy("level", "month", "monthnum")
                .count().orderBy("monthnum", "level");*/

        logs = logs.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
        List<Object> months =new ArrayList<>(List.of(DateFormatSymbols.getInstance().getMonths()));
        months.remove(12);
        System.out.println("DateFormatSymbols.getInstance().getMonths() = " + months);

        logs.groupBy("level").pivot("month", months).count().na().fill(0).show();
        //Create a Pivot Table


        //logs.show();
    }

    public static void multipleGroupingsTest(SparkSession session){
        Dataset<Row> result = session.sql("SELECT level, " +
                "DATE_FORMAT(datetime, 'MMMM') AS month, COUNT(1) AS total FROM logging_table " +
                "GROUP BY level, month ORDER BY " +
                "CAST(FIRST(DATE_FORMAT(datetime, 'M')) AS INT ), level ");

        result.show(100, false);
        result.createOrReplaceTempView("logging_table");
        session.sql("SELECT SUM(total) FROM logging_table").show();
    }

}
