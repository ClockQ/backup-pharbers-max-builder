package com.pharbers.servier

import java.util.UUID

import com.pharbers.spark.phSparkDriver
import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.servier.calc.phServierMaxJob
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.channel.consumer.commonXmppConsumer
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object testServierCalc extends App {
    val job_id: String = "job_id"
    val panel_name: String = "IHD_Panel 201809.csv"
    println(s"panel_name = $panel_name")
    val max_name: String = UUID.randomUUID().toString
    println(s"max_name = $max_name")
    val max_search_name: String = UUID.randomUUID().toString
    println(s"max_search_name = $max_search_name")

    val map: Map[String, String] = Map(
        "panel_path" -> "hdfs:///data/servier/",
        "panel_name" -> panel_name,
        "max_path" -> "hdfs:///workData/Max/",
        "max_name" -> max_name,
        "max_search_name" -> max_search_name,
        "ym" -> "201809",
        "mkt" -> "IHD",
        "job_id" -> job_id,
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "prod_lst" -> "施维雅",
        "p_current" -> "1",
        "p_total" -> "1",
        "panel_delimiter" -> ",",
        "universe_file" -> "hdfs:///data/servier/Servier_Universe_IHD_20181119.csv"
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
    val acter_location: String = xmppFactor.startLocalClient(new commonXmppConsumer)
//    val lactor: ActorSelection = system.actorSelection(acter_location)
    val lactor: ActorSelection = system.actorSelection(xmppFactor.getNullActor)
    val send: channelEntity => Unit = { obj =>
        lactor ! ("lu@localhost#alfred@localhost", obj)
    }

    val result = phServierMaxJob(map)(send).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    phSparkDriver(job_id).stopCurrConn
}