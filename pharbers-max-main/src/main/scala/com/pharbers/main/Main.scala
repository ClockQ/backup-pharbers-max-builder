package com.pharbers.main

import akka.actor.ActorSystem
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.main.PhConsumer.callJobXmppConsumer
import com.pharbers.main.PhConsumer.mainXmppConfBase.xmpp_conf_base

object Main extends App {
    implicit val system: ActorSystem = ActorSystem("maxActor")

    xmppFactor.startLocalClient(new callJobXmppConsumer())
}