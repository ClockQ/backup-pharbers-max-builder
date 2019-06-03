package com.pharbers.max.nhwa

import java.util.UUID
import com.pharbers.spark.phSparkDriver
import com.pharbers.max.nhwa.clean.phNhwaCleanJob
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-05-17 19:12
  */
object test1NhwaClean extends App {
    val job_id: String = "job_id"
    val company_id: String = "5ca069bceeefcc012918ec72"
    val user_id: String = "user_id"

    val clean_path: String = "hdfs:///workData/Clean/"
    val clean_name: String = UUID.randomUUID().toString

    val map: Map[String, String] = Map(
        "job_id" -> job_id,
        "user_id" -> user_id,
        "company_id" -> company_id,

        "cpa_file" -> "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_201809_CPA_20181126.csv",
        "hospital_file" -> "hdfs:///repository/hosp_dis_max",
        "product_file" -> ("hdfs:///repository/prod_etc_dis_max/" + company_id),
        "pha_file" -> "hdfs:///repository/pha",
        "product_match_file" -> "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv",

        "cpa_erd_path" -> clean_path,
        "cpa_erd_name" -> clean_name
    )

    import com.pharbers.max.common.action.sendProgress
    implicit val sd: phSparkDriver = phSparkDriver("test-nhwa-clean")
    val result = phNhwaCleanJob(map).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    {
        import com.pharbers.data.util.Parquet2DF
        Parquet2DF(clean_path + clean_name).show(false)
    }
}
