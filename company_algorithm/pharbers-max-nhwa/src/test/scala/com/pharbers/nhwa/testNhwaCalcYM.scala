package com.pharbers.nhwa

import com.pharbers.spark.phSparkDriver
import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.nhwa.calcym.phNhwaCalcYMJob
import com.pharbers.channel.driver.xmpp.xmppClient
import com.pharbers.channel.consumer.callJobXmppConsumer
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object testNhwaCalcYM extends App {
    val job_id: String = "job_id2"
    val map: Map[String, String] = Map(
        "cpa_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_201804_CPA_.csv",
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
    val acter_location: String = xmppClient.startLocalClient(new callJobXmppConsumer)
    val lactor: ActorSelection = system.actorSelection(acter_location)

    val result = phNhwaCalcYMJob(map)(lactor).perform()
            .asInstanceOf[MapArgs].get("result")
            .asInstanceOf[StringArgs].get
    println(result)

    phSparkDriver(job_id).stopCurrConn
}