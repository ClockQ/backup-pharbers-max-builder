package com.pharbers.channel

import akka.actor.ActorSystem
import com.pharbers.macros.api.commonEntity
import com.pharbers.channel.detail.channelEntity
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.channel.consumer.commonXmppConsumer
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object testXMPP extends App {
    implicit val system: ActorSystem = ActorSystem("maxActor")

    class PhMaxJob extends commonEntity with channelEntity {
        var user_id : String = ""
        var company_id : String = ""
        var date : String = ""
        var call: String = ""
        var job_id: String = ""
        var message: String = ""
        var percentage: Int = 0
        var cpa: String = ""
        var gycx: String = ""
        var not_arrival_hosp_file: String = ""
        var yms = ""
    }

    // 测试首次创建及发送情况
    {
        val xmppconfig: XmppConfigType = Map(
            "xmpp_host" -> "192.168.100.172",
            "xmpp_port" -> "5222",
            "xmpp_user" -> "driver",
            "xmpp_pwd" -> "driver",
            "xmpp_listens" -> "lu@localhost",
            "xmpp_pool_num" -> "2"
        )
        val acter_location = xmppFactor.startLocalClient(new commonXmppConsumer())(system, xmppconfig)
        println(acter_location)
        val sendActor = system.actorSelection(acter_location) // "akka://maxActor/user/cui"
        val nullActor = system.actorSelection(xmppFactor.getNullActor) // "akka://maxActor/user/cui"
        val result = new PhMaxJob
        result.company_id = "a"
        result.user_id = "b"
        result.call = "c"
        result.job_id = "d"
        result.percentage = 1
        sendActor ! ("lu@localhost", result)
        sendActor ! ("alfred@localhost", result)
        sendActor ! result
        nullActor ! result
    }

    // 测试二次创建及发送情况
    {
        val xmppconfig2: XmppConfigType = Map(
            "xmpp_host" -> "192.168.100.172",
            "xmpp_port" -> "5222",
            "xmpp_user" -> "driver",
            "xmpp_pwd" -> "driver",
            "xmpp_listens" -> "lu@localhost",
            "xmpp_pool_num" -> "5"
        )
        val acter_location2 = xmppFactor.startLocalClient(new commonXmppConsumer())(system, xmppconfig2)
        println(acter_location2)
        val sendActor2 = system.actorSelection(acter_location2) // "akka://maxActor/user/cui"
        val result2 = new PhMaxJob
        result2.company_id = "a"
        result2.user_id = "b"
        result2.call = "c"
        result2.job_id = "d"
        result2.percentage = 1
        sendActor2 ! ("lu@localhost", result2)

        xmppFactor.stopLocalClient()(system, xmppconfig2)
        sendActor2 ! ("lu@localhost", result2)
    }


}
