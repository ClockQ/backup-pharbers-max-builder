package com.pharbers.astellas

import com.pharbers.spark.phSparkDriver
import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.astellas.calcym.phAstellasCalcYMJob
import com.pharbers.astellas.panel.phAstellasPanelJob
import com.pharbers.astellas.testAstellasCalc.{map, system}
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.channel.consumer.commonXmppConsumer
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object testAstellasCalcYM extends App {
    val job_id: String = "astl_job_id"
    val map: Map[String, String] = Map(
        "cpa_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_201804_CPA.csv",
        "gycx_file" -> "hdfs:///data/astellas/pha_config_repository1804/Astellas_201804_Gycx_20180703.csv",
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "job_id" -> job_id
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