package com.pharbers.channel.driver.xmpp.xmppImpl

import com.pharbers.util.log.phLogTrait
import akka.actor.{Actor, ActorLogging, Props}
import com.pharbers.channel.detail.channelEntity
import com.pharbers.channel.driver.xmpp.xmppClient

object xmppMsgPool {
    def props(handler: xmppTrait) = Props(new xmppMsgPool(handler))
}

class xmppMsgPool(handler: xmppTrait) extends Actor with ActorLogging with phLogTrait {
    override def receive: Receive = {
        case (_: String, body: AnyRef) => handler.consumeHandler(body.toString)
        case (jr: channelEntity, cli: xmppClient) => cli.broadcastXmppMsg(handler.encodeHandler(jr))
        case msg: Any => phLog("接受非法:"+ msg)
    }
}
