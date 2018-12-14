package com.pharbers.nhwa

import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.nhwa.calcym.phNhwaCalcYMJob
import com.pharbers.pactions.actionbase.{JVArgs, MapArgs}
import com.pharbers.channel.driver.xmpp.xmppClient
import com.pharbers.channel.consumer.callJobXmppConsumer
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType
import com.pharbers.spark.phSparkDriver

object testNhwaCalcYM extends App {
    val map: Map[String, String] = Map(
        "cpa_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_201804_CPA_.csv",
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "job_id" -> "job_id"
    )

    implicit val system = ActorSystem("maxActor")
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

    val result = phNhwaCalcYMJob(map)(lactor).perform(MapArgs(Map().empty))
            .asInstanceOf[MapArgs].get("result").asInstanceOf[JVArgs].get

    new phSparkDriver("job_id").stopCurrConn
}