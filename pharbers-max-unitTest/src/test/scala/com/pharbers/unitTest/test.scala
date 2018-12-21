//package com.pharbers.resultCheck
//
//import java.io.{BufferedReader, File, InputStreamReader}
//import java.util.UUID
//
//import com.pharbers.common.algorithm.max_path_obj
//
//import sys.process._
//import com.pharbers.pactions.actionbase.{DFArgs, pActionTrait}
//import com.pharbers.pactions.excel.input.PhExcelXLSXCommonFormat
//import com.pharbers.pactions.generalactions._
//import com.pharbers.pactions.jobs.{choiceJob, sequenceJob}
//import com.pharbers.panel.nhwa.format.phNhwaCpaFormat
//import com.pharbers.spark.phSparkDriver
//import com.pharbers.unitTest.action2.{executeMaxAction, loadUnitTestJarAction, resultCheckAction, writeCheckResultAction}
//import org.apache.spark.sql.DataFrame
//
//
//object test extends App {
//        //读max结果并筛选输出（20180705）
//        val sparkDriver = phSparkDriver()
//        val maxDF = sparkDriver.sqc.read.format("com.databricks.spark.csv")
//                .option("header", "true") //这里如果在csv第一行有属性的话，没有就是"false"
//                .option("inferSchema", true.toString) //这是自动推断属性列的数据类型。
//                .option("delimiter", 31.toChar.toString)
//                .load("hdfs:///workData/Panel/b79b4cd3-6db5-4ae9-931e-0b915e66e415") //文件的路径
//
//        maxDF.filter("Panel_ID=='PHA0002217'").coalesce(1).write
//                .format("csv")
//                .option("header", value = true)
//                .option("delimiter", 31.toChar.toString)
//                .save(s"/mnt/config/result/" + "PHA0001899筛选结果")
//
//
//    //从git获取数据
//    //    def shellUtil(shStr: String): Unit = {
//    //        val bashs: Array[String] = Array("/bin/sh", "-c", shStr)
//    //        val process = Runtime.getRuntime.exec(bashs)
//    //        process.waitFor()
//    //        val result: BufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream))
//    //        while (result.readLine() != null) {
//    //            println(result.readLine())
//    //        }
//    //    }
//    //
//    //    val path = "/mnt/config/MatchFile/pfizer/pha_config_repository1804/Pfizer_201804_CPA.xlsx"
//    //    val YM = "1804"
//    //    val company = "pfizer"
//    //    val file = new File(path)
//    //    val local_repository_path = "/mnt/config/MatchFile" + "/" + company + "/"
//    //    val repository = "pha_config_" + "/" + company + YM
//    //    if (file.exists()) {
//    //        shellUtil("cd " + "/" + local_repository_path + repository + " && git pull")
//    //    } else {
//    //        val local_repository = new File(local_repository_path)
//    //        if (local_repository.exists()) {
//    //            shellUtil("cd " + local_repository_path + "&& git clone -b pfizer git@192.168.100.137:/mnt/config/pha_repositories/pha_config_repository1804.git")
//    //        } else {
//    //            local_repository.mkdirs()
//    //            shellUtil("cd " + local_repository_path + "&& git clone -b pfizer git@192.168.100.137:/mnt/config/pha_repositories/pha_config_pfizer1804.git")
//    //        }
//    //    }
//    //}
//
////    val load_source_file: sequenceJob = new sequenceJob {
////        val temp_name: String = UUID.randomUUID().toString
////        val temp_dir: String = max_path_obj.p_cachePath
////        override val name = "source_file"
////        override val actions: List[pActionTrait] =
////            xlsxReadingAction[PhExcelXLSXCommonFormat]("/mnt/config/MatchFile/pfizer/pha_config_repository1804/180615辉瑞1804底层检索.xlsx", temp_name) ::
////                    saveCurrenResultAction(temp_dir + temp_name) ::
////                    csv2DFAction(temp_dir + temp_name) :: Nil
////    }
////
////    val load_standard_file: sequenceJob = new sequenceJob {
////        val temp_name: String = UUID.randomUUID().toString
////        val temp_dir: String = max_path_obj.p_cachePath + temp_name + "/"
////        override val actions: List[pActionTrait] = existenceRdd("full_hosp_file") ::
////                csv2DFAction(temp_dir + "full_hosp_file") ::
////                new sequenceJob {
////                    override val name: String = "read_full_hosp_file_job"
////                    override val actions: List[pActionTrait] =
////                        xlsxReadingAction[phNhwaCpaFormat]("", "full_hosp_file") ::
////                                saveCurrenResultAction(temp_dir + "full_hosp_file") ::
////                                csv2DFAction(temp_dir + "full_hosp_file") :: Nil
////                } :: Nil
////        override val name: String = "standard_file"
////    }
////
////    val actions: List[pActionTrait] = {
////        jarPreloadAction() ::
////                setLogLevelAction("ERROR") ::
////                load_source_file ::
////                load_standard_file::
////                Nil
////    }
//
//}
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
