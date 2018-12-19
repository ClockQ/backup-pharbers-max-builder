package com.pharbers.astellas

import java.util.UUID
import com.pharbers.spark.phSparkDriver
import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.channel.driver.xmpp.xmppClient
import com.pharbers.astellas.panel.phAstellasPanelJob
import com.pharbers.channel.consumer.callJobXmppConsumer
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object testAstellasPanel extends App {
    val job_id: String = "astl_job_id"

    val panel_name: String = UUID.randomUUID().toString
    println(s"panel_name = $panel_name")

    def getMktEN(mkt: String): String = {
        mkt match {
            case "阿洛刻市场" => "Allelock"
            case "米开民市场" => "Mycamine"
            case "普乐可复市场" => "Prograf"
            case "佩尔市场" => "Perdipine"
            case "哈乐市场" => "Harnal"
            case "痛风市场" => "Gout"
            case "卫喜康市场" => "Vesicare"
            case "Grafalon市场" => "Grafalon"
            case "前列腺癌市场" => "前列腺癌"
        }
    }

    val map: Map[String, String] = Map(
        "panel_path" -> "hdfs:///workData/Panel/",
        "panel_name" -> panel_name,
        "ym" -> "201804",
        "mkt" -> "阿洛刻市场",
        "mkt_en" -> "Allelock",
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "p_current" -> "1",
        "p_total" -> "1",
        "job_id" -> job_id,
        "prod_lst" -> "安斯泰来",
        "cpa_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_201804_CPA.csv",
        "gycx_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_201804_Gycx_20180703.csv",
        "product_match_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_ProductMatchTable_20180703.csv",
        "markets_match_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_MarketMatchTable_Allelock_20180702.csv",
        "hosp_ID_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_2018_If_panel_all_Allelock_20180703.csv",
        "hospital_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_ThreeSourceTable_20180629.csv"
    )

    implicit val system: ActorSystem = ActorSystem("maxActor")
    implicit val xmppconfig: XmppConfigType = Map(
        "xmpp_host" -> "192.168.100.172",
        "xmpp_port" -> "5222",
        "xmpp_user" -> "cui",
        "xmpp_pwd" -> "cui",
        "xmpp_listens" -> "lu@localhost",
        "xmpp_report" -> "lu@localhost#admin@localhost",
        "xmpp_pool_num" -> "1"
    )
    val acter_location: String = xmppClient.startLocalClient(new callJobXmppConsumer)
//    val lactor: ActorSelection = system.actorSelection(acter_location)
    val lactor: ActorSelection = system.actorSelection("akka://maxActor/user/null")

    val result = phAstellasPanelJob(map)(lactor).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    phSparkDriver(job_id).stopCurrConn
}