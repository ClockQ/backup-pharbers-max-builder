package com.pharbers.unitTest

import java.util.UUID

import org.apache.spark.sql.SparkSession


/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/08/20 17:40
  * @note 一些值得注意的地方
  */
object updown extends App {

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val name = "6b322af4-99e1-44d2-878c-c7a60e38a6a7\n55ad70dd-3ff9-49a0-b9ca-34cf260af005\ndfd2eff1-8b9d-4d41-9384-1e36344c0ad8\ne80f6d60-d374-48ed-91cb-8e3524cfa9db\n5a485b7d-8f8f-43f5-a8aa-586b750e6ead\na094e03f-18c8-4483-bc6f-0648cecbebcf\nf24af630-0608-4a41-b59c-cf6354c61034\n2142b9f6-c473-4d74-abf6-9ad3bb1315b5\n6d69175c-897f-4015-aab6-12e96ae90c33\nfc5da00f-2557-4b29-9212-5488cb669fab\n702513a4-66f5-4940-8a8f-3f0efeb85a49\nefe276f3-06f6-4ccd-bbeb-a3528c1944b0\n856fd7c0-5ad8-4291-856b-6cc702ae2145\n7e4faa3d-4b8f-4680-a7d8-8023bcef3f83\n6bbc5dc3-d180-4a55-b4e2-1acb232c933e\n3cae3bd7-ac51-4453-b299-0574d87206b0\n7c39c5d8-e0b4-42a8-8e6d-535ba923747e\ne0626d92-76fb-4cf1-9802-e7b05675b6d3\n67cf6f07-81a7-4599-9700-ae04a37affdc\n9fd37239-c1f6-4bc8-83a7-bbd8fe34611d\nfb58c762-c342-440b-9f39-fd32d7bc6522\n098ed059-176e-4d99-80f1-4d8c8ca3d47a\necb3dddb-599c-481f-a025-2fdd6783e132"
            .split("\n").map(x => x.trim)

    val path = "/workData/Max"
    val conf = new Configuration
    val hdfs = FileSystem.get(conf)
    val hdfsPath = new Path(path)
    val id = UUID.randomUUID().toString
    val filePathlst = hdfs.listStatus(hdfsPath)
    name.foreach { x =>
       try {
           save(x)
       } catch {
           case e: Exception => e.printStackTrace()
       }
    }
    def save(x: String): Unit ={
        println(x)
        //        val filePath = new Path(s"$path/$x")
        //        FileUtil.copyMerge(hdfs, filePath, hdfs, new Path(s"/user/dcs/MaxMerge/pfizer/1910/$x"), false, conf, null)
        val df = spark.read.parquet(s"hdfs://spark.master:8020/workData/Max/$x").repartition(1).cache()
        val name = df.limit(1).select("MARKET").collect().head.getAs[String](0)
        df.write
                .format("csv")
                .option("encoding", "UTF-8")
                .option("header", value = true)
                .option("delimiter", ",")
                .save(s"file:///D:\\文件\\计算结果\\pfizer1910\\$name")
    }
}
