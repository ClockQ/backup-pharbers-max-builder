package com.pharbers.max.nhwa

import java.util.UUID
import com.pharbers.data.util._
import org.apache.spark.sql.functions._
import com.pharbers.spark.phSparkDriver
import com.pharbers.max.nhwa.max.phNhwaMaxJob
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-05-20 11:28
  */
object test4NhwaMax extends App {
    val job_id: String = "job_id"
    val company_id: String = "5ca069bceeefcc012918ec72"
    val user_id: String = "user_id"

    val max_path: String = "hdfs:///workData/Max/"
    val max_name: String = UUID.randomUUID().toString

    val panel_id: String = "6e57dff1-2cf0-439f-a8f7-4625439d8a8e"

    val map: Map[String, String] = Map(
        "ym" -> "201809",
        "mkt" -> "麻醉市场",
        "job_id" -> job_id,
        "user_id" -> user_id,
        "company_id" -> company_id,
        "prod_name_lst" -> "恩华",

        "panel_file" -> ("hdfs:///workData/Panel/" + panel_id),
        "universe_hosp_file" -> ("/repository/universe_hosp/" + company_id + "/mz"),

        "max_path" -> max_path,
        "max_name" -> max_name
    )

    import com.pharbers.max.common.action.sendProgress
    implicit val sd: phSparkDriver = phSparkDriver("test-nhwa-max")
    val result = phNhwaMaxJob(map).perform()
            .asInstanceOf[MapArgs].get("phNhwaMaxConcretAction")
            .asInstanceOf[DFArgs].get
    println(result)

    val maxDF = result
    lazy val maxTrue = Parquet2DF("/test/qi/qi/max_true")
    lazy val maxTrue2 = CSV2DF("hdfs:///data/nhwa/pha_config_repository1809/Nhwa_201809_Offline_MaxResult_20181126.csv")
    lazy val maxTrue3 = CSV2DF("hdfs:////test/qi/qi/max_result.csv")

    println(maxTrue.count(), maxTrue2.count(), maxTrue3.count(), maxDF.count())

    println(maxTrue.agg(sum("f_units")).first.get(0))
    println(maxTrue.agg(sum("f_sales")).first.get(0))

    maxTrue2.show(2, false)
////    println(maxTrue2.agg(sum("f_units")).first.get(0))
////    println(maxTrue2.agg(sum("f_sales")).first.get(0))
//
    println(maxTrue3.agg(sum("f_units")).first.get(0))
    println(maxTrue3.agg(sum("f_sales")).first.get(0))

    println(maxDF.agg(sum("f_units")).first.get(0))
    println(maxDF.agg(sum("f_sales")).first.get(0))
}
