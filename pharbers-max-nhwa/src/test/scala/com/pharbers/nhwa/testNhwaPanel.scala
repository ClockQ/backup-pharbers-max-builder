package com.pharbers.nhwa

import java.util.UUID

import com.pharbers.spark.phSparkDriver
import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.nhwa.panel.phNhwaPanelJob
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.channel.consumer.commonXmppConsumer
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, StringArgs}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType
import com.pharbers.data.util._
import com.pharbers.nhwa.testNhwaClean.sd

object testNhwaPanel extends App {
//    val job_id: String = "job_id"
//    val panel_name: String = UUID.randomUUID().toString
//    println(s"panel_name = $panel_name")
//
//    val map: Map[String, String] = Map(
//        "panel_path" -> "hdfs:///workData/Panel/",
//        "panel_name" -> panel_name,
//        "ym" -> "201804",
//        "mkt" -> "麻醉市场",
//        "user_id" -> "user_id",
//        "company_id" -> "company_id",
//        "p_current" -> "1",
//        "p_total" -> "1",
//        "job_id" -> job_id,
//        "prod_lst" -> "恩华",
//        "cpa_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_201804_CPA_.csv",
//        "not_arrival_hosp_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_201804_not_arrival_hosp.csv",
//        "not_published_hosp_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_NotPublishedHosp_20180629.csv",
//        "product_match_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_ProductMatchTable_20180629.csv",
//        "fill_hos_data_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_FullHosp_20180629.csv",
//        "markets_match_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_MarketMatchTable_20180629.csv",
//        "hosp_ID_file" ->
//    )
//
//    implicit val system: ActorSystem = ActorSystem("maxActor")
//    implicit val xmppconfig: XmppConfigType = Map(
//        "xmpp_host" -> "192.168.100.172",
//        "xmpp_port" -> "5222",
//        "xmpp_user" -> "driver",
//        "xmpp_pwd" -> "driver",
//        "xmpp_listens" -> "lu@localhost",
//        "xmpp_pool_num" -> "1"
//    )
//    val acter_location: String = xmppFactor.startLocalClient(new commonXmppConsumer())
//    val lactor: ActorSelection = system.actorSelection(acter_location)
////    val lactor: ActorSelection = system.actorSelection(xmppFactor.getNullActor)
//    val send: channelEntity => Unit = {
//        obj => Unit //lactor ! ("lu@localhost#alfred@localhost", obj)
//    }
//
//    val result = phNhwaPanelJob(map)(send).perform()
//            .asInstanceOf[MapArgs].get("panel")
//            .asInstanceOf[DFArgs].get
//    println(result)
//    result.show(false)

//    phSparkDriver(job_id).stopCurrConn

    implicit val sd: phSparkDriver = phSparkDriver("abc")

    import sd.ss.implicits._

//    val cpaERD = Parquet2DF("/workData/Clean/" + "4150679b-3e2f-427b-bcef-3168d00b1bc9")
//    val cpaERD = Parquet2DF("/workData/Clean/" + "b64c14a3-79b7-46b1-bbec-b0bd3e071ecc")
    val cpaERD = Parquet2DF("/workData/Clean/" + "fccb4dce-cd8b-4d64-8805-5e6a4246a40e")
    val missHospERD = Parquet2DF("/repository/miss_hosp" + "/" + "5ca069bceeefcc012918ec72")
    val notPublishedHospERD = Parquet2DF("/repository/not_published_hosp" + "/" + "5ca069bceeefcc012918ec72")
    val fullHospERD = Parquet2DF("/repository/full_hosp" + "/" + "5ca069bceeefcc012918ec72" + "/20180629")
    val sampleHospERD = Parquet2DF("/repository/sample_hosp" + "/" + "5ca069bceeefcc012918ec72" + "/" + "mz")

//    val productMatchDF = CSV2DF("hdfs:///data/nhwa/pha_config_repository1804/Nhwa_ProductMatchTable_20180629.csv")
//    val phaDF = Parquet2DF("hdfs:///repository/pha")
//    val hospDIS = Parquet2DF("/repository/hosp_dis_max")

    val ym = "201809"

    // full hosp
    val cpa = {
        val primalCpa = cpaERD.filter($"YM" === ym)
        val missHosp = missHospERD.filter($"DATE" === ym)
                .drop("DATE")
                .unionByName(notPublishedHospERD)
                .drop("_id")
                .distinct()

        val reducedCpa = primalCpa
                .join(missHosp, primalCpa("HOSPITAL_ID") === missHosp("HOSPITAL_ID"), "left")
                .filter(missHosp("HOSPITAL_ID").isNull)
                .drop(missHosp("HOSPITAL_ID"))

        val result = missHosp
                .join(fullHospERD.filter($"YM" === ym)
                    , missHosp("HOSPITAL_ID") === fullHospERD("HOSPITAL_ID")
                )
                .drop(missHosp("HOSPITAL_ID"))
                .union(reducedCpa)
//        result.show(false)
        result
    }

    // set sample
    val panelERD = cpa
            .join(
                sampleHospERD.filter($"MARKET" === "麻醉市场").filter($"SAMPLE" === "1").select("HOSPITAL_ID")
                , cpa("HOSPITAL_ID") === sampleHospERD("HOSPITAL_ID")
            )
            .drop(sampleHospERD("HOSPITAL_ID"))
            .groupBy("COMPANY_ID", "YM", "HOSPITAL_ID", "PRODUCT_ID")
            .agg(Map("UNITS" -> "sum", "SALES" -> "sum"))
            .withColumnRenamed("sum(UNITS)", "UNITS")
            .withColumnRenamed("sum(SALES)", "SALES")

    panelERD.save2Parquet("/test/qi/qi/panel5")
}