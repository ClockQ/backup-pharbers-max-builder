package com.pharbers.channel

import akka.actor.ActorSystem
import com.pharbers.channel.detail.PhMaxJob
import com.pharbers.channel.driver.xmpp.xmppClient
import com.pharbers.channel.consumer.callJobXmppConsumer
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object testXMPP extends App {
    implicit val system = ActorSystem("maxActor")

    // 测试首次创建及发送情况
    {
        val xmppconfig: XmppConfigType = Map(
            "xmpp_host" -> "192.168.100.172",
            "xmpp_port" -> "5222",
            "xmpp_user" -> "cui",
            "xmpp_pwd" -> "cui",
            "xmpp_listens" -> "alfred@localhost",
            "xmpp_report" -> "lu@localhost#admin@localhost",
            "xmpp_pool_num" -> "1"
        )
        val acter_location = xmppClient.startLocalClient(new callJobXmppConsumer()(system, xmppconfig))(system, xmppconfig)
        println(acter_location)
        val sendActor = system.actorSelection(acter_location) // "akka://maxActor/user/cui"
        val nullActor = system.actorSelection("akka://maxActor/user/null") // "akka://maxActor/user/cui"
        val result = new PhMaxJob
        result.company_id = "a"
        result.user_id = "b"
        result.call = "c"
        result.job_id = "d"
        result.percentage = 1
        sendActor ! result
        nullActor ! result
        Thread.sleep(10000)
        xmppClient.stopLocalClient()(system, xmppconfig)
    }

    Thread.sleep(10000)

    // 测试二次创建及发送情况
    {
        val xmppconfig2: XmppConfigType = Map(
            "xmpp_host" -> "192.168.100.172",
            "xmpp_port" -> "5222",
            "xmpp_user" -> "cui",
            "xmpp_pwd" -> "cui",
            "xmpp_listens" -> "lu@localhost",
            "xmpp_report" -> "lu@localhost#admin@localhost",
            "xmpp_pool_num" -> "1"
        )
        val acter_location2 = xmppClient.startLocalClient(new callJobXmppConsumer()(system, xmppconfig2))(system, xmppconfig2)
        println(acter_location2)
        val sendActor2 = system.actorSelection(acter_location2) // "akka://maxActor/user/cui"
        val result2 = new PhMaxJob
        result2.company_id = "a"
        result2.user_id = "b"
        result2.call = "c"
        result2.job_id = "d"
        result2.percentage = 1
        sendActor2 ! result2
    }
}
