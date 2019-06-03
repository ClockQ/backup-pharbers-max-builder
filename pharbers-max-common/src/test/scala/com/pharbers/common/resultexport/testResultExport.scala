package com.pharbers.common.resultexport

import com.pharbers.spark.phSparkDriver
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}

/**
  * @description:
  * @author: clock
  * @date: 2018-12-30 12:49
  */
object testResultExport extends App {
    val job_id: String = "result_export_job_id"
    val map: Map[String, String] = Map(
        "ym" -> "",
        "mkt" -> "",
        "job_id" -> job_id,
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "p_current" -> "1",
        "p_total" -> "1",
        "max_path" -> "hdfs:///workData/Max/",
        "max_name" -> "b17c7043-531d-4e4b-853b-e435b84977f3",
        "max_delimiter" -> 31.toChar.toString,
        "export_path" -> "hdfs:///workData/Export/",
        "export_name" -> "qilu_1_export",
        "export_delimiter" -> ","
    )

    val send: channelEntity => Unit = { _ => Unit }

    val result = phResultExportJob(map)(send).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    phSparkDriver(job_id).stopSpark()
}