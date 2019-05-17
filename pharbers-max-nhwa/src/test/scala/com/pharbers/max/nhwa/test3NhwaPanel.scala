package com.pharbers.max.nhwa

import java.util.UUID
import com.pharbers.spark.phSparkDriver
import com.pharbers.max.nhwa.panel.phNhwaPanelJob
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, StringArgs}

object test3NhwaPanel extends App {
    val job_id: String = "job_id"
    val company_id: String = "5ca069bceeefcc012918ec72"
    val user_id: String = "user_id"

    val panel_path: String = "hdfs:///workData/Panel/"
    val panel_name: String = UUID.randomUUID().toString

    val clean_id: String = "20bfd585-c889-4385-97ec-a8d4c77d71cc"

    val map: Map[String, String] = Map(
        "ym" -> "201809",
        "mkt" -> "麻醉市场",
        "job_id" -> job_id,
        "user_id" -> user_id,
        "company_id" -> company_id,
        "prod_name_lst" -> "恩华",

        "cpa_file" -> ("hdfs:///workData/Clean/" + clean_id),
        "miss_hosp_file" -> ("hdfs:///repository/miss_hosp/" + company_id),
        "not_publish_hosp_file" -> ("hdfs:///repository/not_published_hosp/" + company_id),
        "full_hosp_file" -> ("hdfs:///repository/full_hosp/" + company_id + "/20180629"),
        "sample_hosp_file" -> ("hdfs:///repository/sample_hosp/" + company_id + "/mz"),

        "panel_path" -> panel_path,
        "panel_name" -> panel_name
    )

    import com.pharbers.max.common.action.sendProgress
    implicit val sd: phSparkDriver = phSparkDriver("test-nhwa-clean")
    val result = phNhwaPanelJob(map).perform()
            .asInstanceOf[MapArgs].get("phNhwaPanelConcretAction")
            .asInstanceOf[DFArgs].get
    println(result)

    {
        import com.pharbers.data.util._
        import org.apache.spark.sql.functions._

        val panel = Parquet2DF(panel_path + panel_name)
        val panelTrue = CSV2DF("/test/qi/qi/1809_panel.csv")

        panel.show(false)

        println(panel.agg(sum("UNITS")).first.get(0))
        println(panel.agg(sum("SALES")).first.get(0))

        println(panelTrue.agg(sum("Units")).first.get(0))
        println(panelTrue.agg(sum("Sales")).first.get(0))
    }
}