package com.pharbers.channel.driver.xmpp

import akka.actor.{Actor, Props}

object xmppNull {
    def props() = Props(new xmppNull())
}

class xmppNull() extends Actor {
    override def receive: Receive = {
        case _ => Unit
    }
}
