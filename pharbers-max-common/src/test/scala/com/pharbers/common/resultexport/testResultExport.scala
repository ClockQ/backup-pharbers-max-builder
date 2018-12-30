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
        "max_name" -> "001e74f9-5c13-469e-9af8-9007cf7fbc676e09e7db-ea8a-4976-b6e4-5c810e61f6a1",
        "max_delimiter" -> 31.toChar.toString,
        "export_path" -> "hdfs:///workData/Export/",
        "export_name" -> "001e74f9-5c13-469e-9af8-9007cf7fbc676e09e7db-ea8a-4976-b6e4-5c810e61f6a1",
        "export_delimiter" -> 31.toChar.toString
    )

    val send: channelEntity => Unit = { _ => Unit }

    val result = phResultExportJob(map)(send).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    phSparkDriver(job_id).stopCurrConn
}