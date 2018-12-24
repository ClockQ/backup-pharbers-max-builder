package com.pharbers.main

import akka.actor.ActorSystem
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.main.PhConsumer.callJobXmppConsumer

object main extends App {
    import com.pharbers.main.PhConsumer.mainXmppConfBase._
    implicit val system: ActorSystem = ActorSystem("maxActor")

    xmppFactor.startLocalClient(new callJobXmppConsumer())

}
