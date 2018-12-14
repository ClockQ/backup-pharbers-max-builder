package com.pharbers.channel

import akka.actor.ActorSystem
import com.pharbers.channel.driver.xmpp.xmppClient
import com.pharbers.channel.consumer.callJobXmppConsumer
import com.pharbers.channel.progress.sendXmppSingleProgress

import scala.language.postfixOps

object testXMPP extends App {
    val system = ActorSystem("maxActor")
    val acter_location = xmppClient.startLocalClient(system, new callJobXmppConsumer(system))
    println(acter_location)

    val sendActor = system.actorSelection(acter_location) // "akka://maxActor/user/xmpp"

    val a = sendXmppSingleProgress("company_id", "user_id", "call", "job_id")(sendActor).singleProgress
    a(Map("progress" -> 1))
}
