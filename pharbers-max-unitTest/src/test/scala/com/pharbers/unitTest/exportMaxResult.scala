package com.pharbers.unitTest

import com.pharbers.spark.phSparkDriver
import com.pharbers.spark.session.spark_conn_instance
import com.pharbers.spark.util.readParquet
import com.pharbers.util.log.phLogTrait.phDebugLog
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import scala.io.Source

object exportMaxResult extends App{
    val sparkDriver: phSparkDriver = phSparkDriver("dcs test")
    import sparkDriver.ss.implicits._
    implicit val conn_instance: spark_conn_instance = sparkDriver.conn_instance
    val file = Source.fromFile("D:\\文件\\bare\\result").getLines()
    val conf = new Configuration
    val hdfs = FileSystem.get(conf)
    file.zipWithIndex.foreach(x => {
        phDebugLog("第" + x._2)
        val df = sparkDriver.setUtil(readParquet()).readParquet("/workData/Max/" + x._1)
        val name = df.select("Date", "MARKET").take(1).mkString(",")
        phDebugLog(name)
        df.write.option("header", value = true).csv("/workData/Export/20190421bare/" +name)
        FileUtil.copyMerge(hdfs, new Path("/workData/Export/20190421bare/" + name), hdfs, new Path("/test/dcs/Merge/20190421bare/" + name + ".csv"),true,conf,null)
    })
    hdfs.copyToLocalFile(false,new Path("/test/dcs/Merge/20190421bare/"), new Path("D:\\文件\\bare"),true)

}
