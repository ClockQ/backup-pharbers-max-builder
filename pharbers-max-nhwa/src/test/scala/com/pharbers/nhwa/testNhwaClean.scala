package com.pharbers.nhwa

import java.util.UUID

import com.pharbers.spark.phSparkDriver
import com.pharbers.nhwa.clean.phNhwaCleanJob
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, StringArgs}

object testNhwaClean extends App {
    val job_id: String = "job_id"
    val company_id: String = "5ca069bceeefcc012918ec72"
    val clean_name: String = UUID.randomUUID().toString
    println(s"clean_name = $clean_name")

    val map: Map[String, String] = Map(
        "job_id" -> job_id,
        "company_id" -> company_id,
        "user_id" -> "user_id",
        "p_current" -> "1",
        "p_total" -> "1",
        "cpa_file" -> "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_201809_CPA_20181126.csv",
        "hospital_file" -> "hdfs:///repository/hosp_dis_max",
        "product_file" -> ("hdfs:///repository/prod_etc_dis_max/" + company_id),
        "pha_file" -> "hdfs:///repository/pha",
        "product_match_file" -> "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv",
        "cpa_erd_path" -> "hdfs:///workData/Clean/",
        "cpa_erd_name" -> clean_name
    )

    val send: channelEntity => Unit = {
        obj => Unit //lactor ! ("lu@localhost#alfred@localhost", obj)
    }

    val result = phNhwaCleanJob(map)(send).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    import com.pharbers.data.util.Parquet2DF

    implicit val sd: phSparkDriver = phSparkDriver("abc")
    Parquet2DF("/workData/Clean/" + clean_name)(sd).show(false)
}