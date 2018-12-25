package com.pharbers.nhwa

import java.util.UUID

import com.pharbers.spark.phSparkDriver
import com.pharbers.nhwa.calc.phNhwaMaxJob
import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.channel.consumer.commonXmppConsumer
import com.pharbers.channel.detail.channelEntity
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object testNhwaCalc extends App {
    val job_id: String = "job_id"
    val panel_name: String = "b436b2f3-d0ef-4a16-bdfc-b175745ef292"
    println(s"panel_name = $panel_name")
    val max_name: String = UUID.randomUUID().toString
    println(s"max_name = $max_name")
    val max_search_name: String = UUID.randomUUID().toString
    println(s"max_search_name = $max_search_name")

    val map: Map[String, String] = Map(
        "panel_path" -> "hdfs:///workData/Panel/",
        "panel_name" -> panel_name,
        "max_path" -> "hdfs:///workData/Panel/",
        "max_name" -> max_name,
        "max_search_name" -> max_search_name,
        "ym" -> "201804",
        "mkt" -> "麻醉市场",
        "job_id" -> job_id,
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "prod_lst" -> "恩华",
        "p_current" -> "1",
        "p_total" -> "1",
        "universe_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_universe_麻醉市场_20180705.csv"
    )

    implicit val system: ActorSystem = ActorSystem("maxActor")
    implicit val xmppconfig: XmppConfigType = Map(
        "xmpp_host" -> "192.168.100.172",
        "xmpp_port" -> "5222",
        "xmpp_user" -> "driver",
        "xmpp_pwd" -> "driver",
        "xmpp_listens" -> "lu@localhost",
        "xmpp_pool_num" -> "1"
    )
    val acter_location: String = xmppFactor.startLocalClient(new commonXmppConsumer)
    val lactor: ActorSelection = system.actorSelection(acter_location)
//    val lactor: ActorSelection = system.actorSelection(xmppFactor.getNullActor)
    val send: channelEntity => Unit = {
        obj => lactor ! ("lu@localhost#alfred@localhost", obj)
    }

    val result = phNhwaMaxJob(map)(send).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    phSparkDriver(job_id).stopCurrConn
}