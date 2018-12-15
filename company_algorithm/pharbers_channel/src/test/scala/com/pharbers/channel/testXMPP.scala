package com.pharbers.channel

import akka.pattern.ask
import akka.actor.ActorSystem
import akka.util.Timeout
import com.pharbers.channel.consumer.callJobXmppConsumer
import com.pharbers.channel.detail.{PhMaxJob, channelEntity}
import com.pharbers.channel.driver.xmpp.xmppClient
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

import scala.concurrent.Await

object testXMPP extends App {
    implicit val system = ActorSystem("maxActor")
    implicit val xmppconfig: XmppConfigType = Map(
        "xmpp_host" -> "192.168.100.172",
        "xmpp_port" -> "5222",
        "xmpp_user" -> "cui",
        "xmpp_pwd" -> "cui",
        "xmpp_listens" -> "alfred@localhost#lu@localhost",
        "xmpp_report" -> "lu@localhost#admin@localhost",
        "xmpp_pool_num" -> "1"
    )
    val acter_location = xmppClient.startLocalClient(new callJobXmppConsumer)
    val xmppconfig2: XmppConfigType = Map(
        "xmpp_host" -> "192.168.100.172",
        "xmpp_port" -> "5222",
        "xmpp_user" -> "alfred",
        "xmpp_pwd" -> "alfred",
        "xmpp_listens" -> "alfred@localhost#lu@localhost",
        "xmpp_report" -> "lu@localhost#admin@localhost",
        "xmpp_pool_num" -> "1"
    )
    val acter_location2 = xmppClient.startLocalClient(new callJobXmppConsumer()(system, xmppconfig2))(system, xmppconfig2)
    val acter_location3 = xmppClient.startLocalClient(new callJobXmppConsumer()(system, xmppconfig2))(system, xmppconfig2)
    println(acter_location)
    println(acter_location2)
    println(acter_location3)

    val sendActor = system.actorSelection(acter_location2) // "akka://maxActor/user/xmpp"
    val result = new PhMaxJob
    result.company_id = "a"
    result.user_id = "b"
    result.call = "c"
    result.job_id = "d"
    result.percentage = 1
    sendActor ! result


//    import scala.concurrent.duration._
//    implicit val t: Timeout = 10 seconds
//    val r = sendActor ? result
//    Await.result(r.mapTo[channelEntity], t.duration)
//    xmppClient.stopLocalClient()(system, xmppconfig2)
//
//    val acter_location4 = xmppClient.startLocalClient(new callJobXmppConsumer()(system, xmppconfig2))(system, xmppconfig2)
//    println(acter_location4)
//    val sendActor4 = system.actorSelection(acter_location4)
//    sendActor4 ! result
}
