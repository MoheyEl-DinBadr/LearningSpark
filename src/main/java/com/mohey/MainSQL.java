package com.mohey;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

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

        Runtime.getRuntime().addShutdownHook(new Thread(() -> sparkSession.close()));

        Dataset<Row> dataset = sparkSession.read()
                .option("header", true).csv("src/main/resources/exams/students.csv");


        /*Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");
        modernArtResults.show();*/

        /*Dataset<Row> modernArtResults = dataset.filter((FilterFunction<Row>) element -> element.getAs("subject").equals("Modern Art") && Integer.parseInt(element.getAs("year")) >= 2007);
        modernArtResults.show();*/

        /*dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007))).show();*/

        dataset.createOrReplaceTempView("my_students_table");
        //sparkSession.sql("SELECT * FROM my_students_table WHERE subject = 'Modern Art' AND year >= 2007").show();
        //sparkSession.sql("SELECT max(score) as MAX FROM my_students_table WHERE subject = 'French'").show();
        sparkSession.sql("SELECT DISTINCT year as years FROM my_students_table ORDER BY years DESC").show();

        while(true);

    }
}
