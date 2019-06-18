package com.pharbers.pfizer

import java.util.UUID

import akka.actor.ActorSystem
import com.pharbers.channel.detail.channelEntity
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.pfizer.panel.phPfizerPanelJob
import com.pharbers.spark.phSparkDriver

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author EDZ
  * @version 0.0
  * @since 2019/06/18 14:44
  * @note 一些值得注意的地方
  */
object testPfizerAI_DPanel extends App {
    val job_id: String = "pfizer_inf_job_id"

    val panel_name: String = UUID.randomUUID().toString
    println(s"panel_name = $panel_name")

    val map: Map[String, String] = Map(
        "panel_path" -> "hdfs:///workData/Panel/",
        "panel_name" -> panel_name,
        "ym" -> "201804",
        "mkt" -> "AI_D",
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "p_current" -> "1",
        "p_total" -> "1",
        "job_id" -> job_id,
        "prod_lst" -> "辉瑞",
        "cpa_file" -> "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_201804_CPA.csv",
        "gycx_file" -> "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_201804_Gycx.csv",
        "hosp_ID_file" -> "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_2018_If_panel_all_AI_D.csv",
        "product_match_file" -> "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_ProductMatchTable.csv",
        "markets_match_file" -> "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_MarketMatchTable.csv",
        "fill_hos_data_file" -> "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_2018_FullHosp.txt",
        "pfc_match_file" -> "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_PackidMatch.csv",
        "not_arrival_hosp_file" -> "hdfs:///data/pfizer/pha_config_repository1804/missingHospital.csv"
    )

    implicit val system: ActorSystem = ActorSystem("maxActor")
    implicit val xmppconfig: XmppConfigType = Map(
        "xmpp_host" -> "192.168.100.172",
        "xmpp_port" -> "5222",
        "xmpp_user" -> "cui",
        "xmpp_pwd" -> "cui",
        "xmpp_listens" -> "lu@localhost",
        "xmpp_pool_num" -> "1"
    )
    //    val acter_location: String = xmppFactor.startLocalClient(new commonXmppConsumer)
    //    val lactor: ActorSelection = system.actorSelection(acter_location)
    //    val lactor: ActorSelection = system.actorSelection(xmppFactor.getNullActor)
    val send: channelEntity => Unit = { obj =>
        ("lu@localhost#alfred@localhost", obj)
    }

    val result = phPfizerPanelJob(map)(send).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    phSparkDriver(job_id).stopCurrConn
}
