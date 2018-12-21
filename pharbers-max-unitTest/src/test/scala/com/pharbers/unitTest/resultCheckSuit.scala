//package com.pharbers.resultCheck
//
//import java.util.Date
//
//import org.scalatest.FunSuite
//import java.text.SimpleDateFormat
//
//import com.pharbers.pactions.actionbase.StringArgs
//import com.pharbers.unitTest.startTest
//import com.pharbers.spark.phSparkDriver
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
//
//class resultCheckSuit extends FunSuite {
//    test("result check") {
//        val dateformat = new SimpleDateFormat("MM-dd HH:mm:ss")
//        println(s"开始检查时间" + dateformat.format(new Date()))
//        println()
//
//        val totalResult = startTest().writeTotalResult()
//        println(totalResult.asInstanceOf[StringArgs].get)
//
//        println()
//        println(s"结束检查时间" + dateformat.format(new Date()))
//    }
//
//
//    //    test("sparkDriver"){
//    //        System.setProperty("HADOOP_USER_NAME","spark")
//    //        val conf = new SparkConf()
//    //                .setAppName("hadoop")
//    //                .set("spark.yarn.jars", "hdfs://spark.master:9000/jars/sparkJars")
//    //                .set("spark.yarn.archive", "hdfs://spark.master:9000/jars/sparkJars")
//    //                .set("yarn.resourcemanager.address", "spark.master:8032")
//    //                .set("yarn.resourcemanager.scheduler.address", "spark.master:8030")
//    //                //            .set("spark.executorEnv.mapreduce.job.user.name", "hadoop")
//    //                //            .setMaster("yarn-cluster")
//    //                .setMaster("yarn")
//    //                .set("yarn.resourcemanager.hostname", "spark.master")
//    //                .set("spark.yarn.dist.files", "hdfs://spark.master:9000/config")
//    ////                .setJars(List("./target/pharbers-unitTest-0.1.jar"))
//    //                //            .set("spark.files", "localhost:/usr/soft/spark/jars")
//    //                //            .set("spark.jars", "localhost:/usr/soft/spark/jars")
//    //                .set("spark.executor.memory", "2g")
//    //                //            .set("yarn.resourcemanager.hostname", "hadoop")
//    //                .set("spark.driver.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,adress=5005")
//    //                .set("spark.executor.extraJavaOptions",
//    //                    """
//    //                      | -XX:+UseG1GC -XX:+PrintFlagsFinal
//    //                      | -XX:+PrintReferenceGC -verbose:gc
//    //                      | -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
//    //                      | -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions
//    //                      | -XX:+G1SummarizeConcMark
//    //                      | -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=1
//    //                    """.stripMargin)
//    //
//    //        val spark_session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//    //        val spark_context: SparkContext = spark_session.sparkContext
//    //        val spark_sql_context: SQLContext = spark_session.sqlContext
//    //        val maxDF: DataFrame = spark_sql_context.read.format("com.databricks.spark.csv")
//    //                .option("header", "true") //这里如果在csv第一行有属性的话，没有就是"false"
//    //                .option("inferSchema", true.toString) //这是自动推断属性列的数据类型。
//    //                .option("delimiter", 31.toChar.toString)
//    //                .load("hdfs:///test/part-r-00000-d4b3d09e-a2fb-44a3-809b-51041511c129.csv") //文件的路径
//    //        //                    .load("/usr/local/part-r-00000-d4b3d09e-a2fb-44a3-809b-51041511c129.csv")
//    //        maxDF.show(false)
//    //        //    maxDF.filter("Panel_ID=='PHA0001899'").coalesce(1).write
//    //        //            .format("csv")
//    //        //            .option("header", value = true)
//    //        //            .option("delimiter", 31.toChar.toString)
//    //        //            .save(s"/mnt/config/result/" + "PHA0001899筛选结果")
//    //    }
//
//}
