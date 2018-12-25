package com.pharbers.channel.consumer

import com.pharbers.macros._
import akka.actor.ActorSystem
import akka.util.Timeout
import io.circe.syntax._
import scala.language.postfixOps
import scala.concurrent.duration._
import com.pharbers.channel.detail.channelEntity
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppTrait
import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
import com.pharbers.macros.convert.jsonapi.JsonapiMacro._
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

class commonXmppConsumer() extends xmppTrait with CirceJsonapiSupport {
    override val encodeHandler: channelEntity => String = obj =>
        toJsonapi(obj).asJson.noSpaces

    override val decodeHandler: String => channelEntity = _ => ???

    override val consumeHandler: (String, String) => Unit = (from, input) => {
        println(s"接受$from 发送的信息: $input")
    }
}
