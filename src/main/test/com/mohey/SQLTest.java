package com.mohey;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.WrappedArray;


import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Mohey El-Din Badr
 * @date 7/26/21 2:18 PM
 * @email MoheyElDin.Badr@gmail.com
 */
public class SQLTest {

    private static SparkSession spark;
    private static List<Row> inMemory = new ArrayList();
    private static Dataset<Row> dataset;

    @BeforeAll
    public static void init(){
        spark = SparkSession.builder().master("local[1]").appName("SQLTest").getOrCreate();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
        StructField[] structFields = new StructField[2];
        structFields[0] = new StructField("level", DataTypes.StringType, false, Metadata.empty());
        structFields[1] = new StructField("dateTime", DataTypes.StringType, false, Metadata.empty());
        StructType structType = new StructType(structFields);
        dataset = spark.createDataFrame(inMemory, structType);

        dataset.createOrReplaceTempView("logging_table");
    }
    @Test
    public void dataCount(){
        assertEquals(dataset.count(),5);
    }

    @Test
    public void grouping(){
        Dataset<Row> result = spark.sql("SELECT level, COUNT(dateTime) as num FROM logging_table GROUP BY level");
        result.collectAsList().forEach(element ->{
            String level = element.getAs("level");

            switch(level){
                case "INFO" : assertEquals((Long)element.getAs("num"),1l);
                    break;
                case "WARN":
                case "FATAL":
                    assertEquals((Long)element.getAs("num"),2l);
                    break;
            }
        });
    }

    @Test
    public void dataFormattingTest(){
        spark.sql("SELECT level, DATE_FORMAT(dateTime, 'MMM') AS month FROM logging_table").show();
    }

    @Test
    public void multipleGroupingsTest(){
        spark.sql("SELECT level, DATE_FORMAT(dateTime, 'MMMM') AS month, COUNT(1) AS total FROM logging_table GROUP BY level, month").show();
    }

    @Test
    public void aggregate(){
        Dataset<Row> result = spark.sql("SELECT level, COLLECT_LIST(dateTime) as events FROM logging_table GROUP BY level");
        result.collectAsList().forEach(element ->{
            String level = element.getAs("level");
            switch(level){
                case "INFO":assertEquals(((WrappedArray)element.getAs("events")).size(),1);
                    break;
                case "WARN":
                case "FATAL":
                    assertEquals(((WrappedArray)element.getAs("events")).size(),2);
                    break;
            }
        });
    }
}
