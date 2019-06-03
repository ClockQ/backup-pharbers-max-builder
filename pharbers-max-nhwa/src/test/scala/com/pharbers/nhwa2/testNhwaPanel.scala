package com.pharbers.nhwa2

import java.util.UUID

import com.pharbers.spark.phSparkDriver
import com.pharbers.nhwa2.panel.phNhwaPanelJob
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}
import com.pharbers.data.util.ParquetLocation.{HOSP_DIS_LOCATION, HOSP_PHA_LOCATION}
import org.apache.spark.sql.types.LongType


object testNhwaPanel extends App {

    import com.pharbers.data.util._

    lazy val old = {
        val job_id: String = "job_id"
        val panel_name: String = UUID.randomUUID().toString
        println(s"panel_name = $panel_name")

        val map: Map[String, String] = Map(
            "panel_path" -> "hdfs:///workData/Panel/",
            "panel_name" -> panel_name,
            "ym" -> "201809",
            "mkt" -> "麻醉市场",
            "user_id" -> "user_id",
            "company_id" -> "company_id",
            "p_current" -> "1",
            "p_total" -> "1",
            "job_id" -> job_id,
            "prod_lst" -> "恩华",
            "cpa_file" -> "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_201809_CPA_20181126.csv",
            "not_arrival_hosp_file" -> "hdfs:///data/nhwa/pha_config_repository1809/Missing_Hospital.csv",
            "not_published_hosp_file" -> "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_2018_NotPublishedHosp_20180629.csv",
            "product_match_file" -> "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv",
            "fill_hos_data_file" -> "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_2018_FullHosp_20180629.csv",
            "markets_match_file" -> "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_MarketMatchTable_20180629.csv",
            "hosp_ID_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_If_panel_all_麻醉市场_20180629.csv"
        )


        val send: channelEntity => Unit = {
            obj => Unit //lactor ! ("lu@localhost#alfred@localhost", obj)
        }

        val result = phNhwaPanelJob(map)(send).perform()
                .asInstanceOf[MapArgs].get("panel")
                .asInstanceOf[DFArgs].get
        println(result)
        result.show(false)
//    result.save2Parquet("/test/qi/qi/panel_true")
    }
//old

    lazy val nnn = {
        val ym = "201809"
        val job_id: String = "job_id"
        val company_id: String = "5ca069bceeefcc012918ec72"
        val user_id: String = "user_id"
        val clean_id: String = "6e57dff1-2cf0-439f-a8f7-4625439d8a8e"

        implicit val sd: phSparkDriver = phSparkDriver("abc")

        import sd.ss.implicits._
        import org.apache.spark.sql.functions._

        val cpaERD = Parquet2DF("/workData/Clean/" + clean_id).cache()
        val missHospERD = Parquet2DF("/repository/miss_hosp" + "/" + company_id).cache()
        val notPublishedHospERD = Parquet2DF("/repository/not_published_hosp" + "/" + company_id).cache()
        val fullHospERD = Parquet2DF("/repository/full_hosp" + "/" + company_id + "/20180629").cache()
        val sampleHospERD = Parquet2DF("/repository/sample_hosp" + "/" + company_id + "/" + "mz").cache()

        // full hosp
        val cpa = { // 7235 => 7235
            val primalCpa = cpaERD.filter($"YM" === ym)
//            println(primalCpa.count())// 6975 => 6975

            val missHosp = missHospERD.filter($"DATE" === ym)
                    .drop("DATE")
                    .unionByName(notPublishedHospERD)
                    .drop("_id")
                    .distinct()
//            println(missHosp.count())// 85 => 85

            val reducedCpa = primalCpa
                    .join(missHosp, primalCpa("HOSPITAL_ID") === missHosp("HOSPITAL_ID"), "left")
                    .filter(missHosp("HOSPITAL_ID").isNull)
                    .drop(missHosp("HOSPITAL_ID"))
//            println(reducedCpa.count()) // 6961 => 6961

            val result = fullHospERD.filter($"YM" === ym)
                    .join(missHosp, fullHospERD("HOSPITAL_ID") === missHosp("HOSPITAL_ID"))//274 => 274
                    .drop(missHosp("HOSPITAL_ID"))
                    .union(reducedCpa)
                    .drop("_id")
                    .generateId
//            println(result.count()) // 7235 => 7235

            result
        }

        // set sample
        val panelERD = {
            cpa
                    .join(
                        sampleHospERD.filter($"MARKET" === "麻醉市场").filter($"SAMPLE" === "1") //1038 => 1035
                        , cpa("HOSPITAL_ID") === sampleHospERD("HOSPITAL_ID")
                    )
                    .filter(sampleHospERD("HOSPITAL_ID").isNotNull)//6305 => 6305
                    .drop(cpa("HOSPITAL_ID"))
                    .groupBy("COMPANY_ID", "YM", "HOSPITAL_ID", "PRODUCT_ID")
                    .agg(Map("UNITS" -> "sum", "SALES" -> "sum"))
                    .withColumnRenamed("sum(UNITS)", "UNITS")
                    .withColumnRenamed("sum(SALES)", "SALES")
        }

        panelERD.show(false)


    }
//    nnn


}