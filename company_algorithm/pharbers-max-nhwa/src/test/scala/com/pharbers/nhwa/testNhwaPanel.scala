package com.pharbers.nhwa

import java.util.UUID

import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.channel.consumer.callJobXmppConsumer
import com.pharbers.channel.driver.xmpp.xmppClient
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType
import com.pharbers.nhwa.panel.phNhwaPanelJob
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.spark.phSparkDriver

object testNhwaPanel extends App {

    val map: Map[String, String] = Map(
        "panel_path" -> "hdfs:///workData/Panel/",
        "panel_name" -> UUID.randomUUID().toString,
        "ym" -> "201804",
        "mkt" -> "麻醉市场",
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "p_current" -> "1",
        "p_total" -> "1",
        "job_id" -> "job_id",
        "prod_lst" -> "恩华",
        "cpa_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_201804_CPA_.csv",
        "not_arrival_hosp_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_201804_not_arrival_hosp.csv",
        "not_published_hosp_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_NotPublishedHosp_20180629.csv",
        "product_match_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_ProductMatchTable_20180629.csv",
        "fill_hos_data_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_FullHosp_20180629.csv",
        "markets_match_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_MarketMatchTable_20180629.csv",
        "hosp_ID_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_If_panel_all_麻醉市场_20180629.csv"
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
    val lactor: ActorSelection = system.actorSelection(acter_location)

    val result = phNhwaPanelJob(map)(lactor).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    phSparkDriver("job_id").stopCurrConn
}