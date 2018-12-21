package com.pharbers.main

import akka.actor.ActorSystem
import com.pharbers.main.entity.PhMaxJob
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.main.consumer.callJobXmppConsumer
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object testXmppConsumer extends App {
    implicit val system = ActorSystem("maxActor")

    val xmppconfig: XmppConfigType = Map(
        "xmpp_host" -> "192.168.100.172",
        "xmpp_port" -> "5222",
        "xmpp_user" -> "cui",
        "xmpp_pwd" -> "cui",
        "xmpp_listens" -> "alfred@localhost",
        "xmpp_report" -> "alfred@localhost",
        "xmpp_pool_num" -> "1"
    )
    val acter_location = xmppFactor.startLocalClient(new callJobXmppConsumer()(system, xmppconfig))(system, xmppconfig)
    println(acter_location)
    val sendActor = system.actorSelection(acter_location) // "akka://maxActor/user/cui"
    val nullActor = system.actorSelection(xmppFactor.getNullActor) // "akka://maxActor/user/cui"
    val result = new PhMaxJob
    result.company_id = "a"
    result.user_id = "b"
    result.call = "c"
    result.job_id = "d"
    result.percentage = 1
    sendActor ! result
    nullActor ! result
//    xmppFactor.stopLocalClient()(system, xmppconfig)
}
