package com.pharbers.pfizer

import java.util.UUID
import akka.actor.ActorSystem
import com.pharbers.spark.phSparkDriver
import com.pharbers.pfizer.calc.phPfizerMaxJob
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object testPfizerINFCalc extends App {
    val job_id: String = "pfizer_inf_job_id"
    val panel_name: String = "inf_0617"
    println(s"panel_name = $panel_name")
    val max_name: String = UUID.randomUUID().toString
    println(s"max_name = $max_name")
    val max_search_name: String = UUID.randomUUID().toString
    println(s"max_search_name = $max_search_name")

    val map: Map[String, String] = Map(
        "panel_path" -> "hdfs:///workData/Panel/",
        "panel_name" -> panel_name,
        "max_path" -> "hdfs:///workData/Max/",
        "max_name" -> max_name,
        "max_search_name" -> max_search_name,
        "ym" -> "201804",
        "mkt" -> "INF",
        "job_id" -> job_id,
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "prod_lst" -> "辉瑞",
        "p_current" -> "1",
        "p_total" -> "1",
        "universe_file" -> "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_Universe_INF.csv"
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

    val result = phPfizerMaxJob(map)(send).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    phSparkDriver(job_id).stopCurrConn
}