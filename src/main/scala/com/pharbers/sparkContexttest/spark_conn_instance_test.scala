package com.pharbers.sparkContexttest

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object spark_conn_instance_test extends App {
    System.setProperty("HADOOP_USER_NAME", "spark")
    private val conf = new SparkConf()
            .setAppName("hadoop")
            .setMaster("yarn")
            .set("spark.yarn.jars", "hdfs:///jars/sparkJars")
            .set("spark.yarn.archive", "hdfs:///jars/sparkJars")
            .set("yarn.resourcemanager.hostname", "spark.master")
            .set("yarn.resourcemanager.address", "spark.master:8032")
            .set("spark.yarn.dist.files", "hdfs://spark.master:9000/config")
            .setJars(List("./target/pharbers-max-0.1.jar"))
            //            .set("spark.files", "localhost:/usr/soft/spark/jars")
            //            .set("spark.jars", "localhost:/usr/soft/spark/jars")
            .set("spark.executor.memory", "2g")
            //            .set("yarn.resourcemanager.hostname", "hadoop")
            .set("spark.driver.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,adress=5005")
            .set("spark.executor.extraJavaOptions",
                """
                  | -XX:+UseG1GC -XX:+PrintFlagsFinal
                  | -XX:+PrintReferenceGC -verbose:gc
                  | -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
                  | -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions
                  | -XX:+G1SummarizeConcMark
                  | -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=1
                """.stripMargin)
    
    val spark_session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val spark_context: SparkContext = spark_session.sparkContext
    val spark_sql_context: SQLContext = spark_session.sqlContext
    
    //    val data = spark_context.textFile("hdfs:///data/pfizer/pha_config_repository1804/Pfizer_2018_FullHosp.txt")
    //    import spark_sql_context.implicits._
    //    val df = data.toDF()
    //    df.show(false)
    
    import spark_session.implicits._
    
    val testDF: DataFrame = spark_sql_context.read.format("com.databricks.spark.csv")
            .option("header", "true") //这里如果在csv第一行有属性的话，没有就是"false"
            //            .option("inferSchema", true.toString) //这是自动推断属性列的数据类型。
            .option("delimiter", ",")
            .load("hdfs:///data/pfizer/pha_config_repository1804/Pfizer_201804_CPA.csv") //文件的路径
    
    testDF.na.fill(value = "0", cols = Array("VALUE", "STANDARD_UNIT"))
            .withColumn("PRODUCT_NAME", when(col("PRODUCT_NAME").isNull, col("MOLE_NAME"))
                    .otherwise(col("PRODUCT_NAME")))
            .withColumn("MONTH", 'MONTH.cast(IntegerType))
            .withColumn("MONTH", when(col("MONTH").>=(10), col("MONTH"))
                    .otherwise(concat(col("MONTH").*(0).cast("int"), col("MONTH"))))
            .withColumn("min1", concat(col("PRODUCT_NAME"), col("DOSAGE"), col("PACK_DES"), col("PACK_NUMBER"), col("CORP_NAME")))
            .withColumn("ym", concat(col("YEAR"), col("MONTH")))
            .show(false)
    
//    testDF.na.fill(Map("VALUE" -> "0", "STANDARD_UNIT" -> "0"))
//            .na.fill(value = "0", cols = Array("VALUE"))
//            .withColumn("HOSP_ID", when(col("HOSP_ID").isNull, col("MONTH")).otherwise(col("HOSP_ID")))
//            .filter("HOSP_ID is null")
//            .filter("STANDARD_UNIT is null")
//            .filter("STANDARD_UNIT == ''")
//            .filter("PRODUCT_NAME == ''")
//            .filter("VALUE == ''")
//            .show(false)
    
    //    maxDF.filter("Panel_ID=='PHA0001899'").coalesce(1).write
    //            .format("csv")
    //            .option("header", value = true)
    //            .option("delimiter", 31.toChar.toString)
    //            .save(s"/mnt/config/result/" + "PHA0001899筛选结果")
}
