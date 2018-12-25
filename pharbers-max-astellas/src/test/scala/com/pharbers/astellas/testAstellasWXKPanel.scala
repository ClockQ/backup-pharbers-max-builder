package com.pharbers.astellas

import java.util.UUID

import com.pharbers.spark.phSparkDriver
import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.astellas.panel.phAstellasPanelJob
import com.pharbers.astellas.testAstellasCalcYM.{map, system}
import com.pharbers.channel.consumer.commonXmppConsumer
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object testAstellasWXKPanel extends App {
    val job_id: String = "astl_job_id"

    val panel_name: String = UUID.randomUUID().toString
    println(s"panel_name = $panel_name")

    val map: Map[String, String] = Map(
        "panel_path" -> "hdfs:///workData/Panel/",
        "panel_name" -> panel_name,
        "ym" -> "201804",
        "mkt" -> "卫喜康市场",
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "p_current" -> "1",
        "p_total" -> "1",
        "job_id" -> job_id,
        "prod_lst" -> "安斯泰来",
        "cpa_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_201804_CPA.csv",
        "gycx_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_201804_Gycx_20180703.csv",
        "product_match_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_ProductMatchTable_20180703.csv",
        "markets_match_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_MarketMatchTable_Vesicare.csv",
        "hosp_ID_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_2018_If_panel_all_Vesicare_20180703.csv",
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
    val acter_location: String = xmppFactor.startLocalClient(new commonXmppConsumer)
    //    val lactor: ActorSelection = system.actorSelection(acter_location)
    val lactor: ActorSelection = system.actorSelection(xmppFactor.getNullActor)
    val send: channelEntity => Unit = { obj =>
        lactor ! ("lu@localhost#alfred@localhost", obj)
    }

    val result = phAstellasPanelJob(map)(send).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    phSparkDriver(job_id).stopCurrConn
}