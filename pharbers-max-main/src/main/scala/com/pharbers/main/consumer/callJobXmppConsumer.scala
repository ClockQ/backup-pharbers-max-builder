package com.pharbers.main.consumer

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.pharbers.channel.detail.channelEntity
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppTrait
import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
import com.pharbers.jsonapi.model._
import com.pharbers.macros._
import com.pharbers.macros.convert.jsonapi.JsonapiMacro._
import io.circe.syntax._
import scala.concurrent.duration._
import scala.language.postfixOps

class callJobXmppConsumer()(implicit context: ActorSystem, xmppConfig: XmppConfigType)
        extends xmppTrait with CirceJsonapiSupport {
    implicit val t: Timeout = 5 hours

    override val encodeHandler: channelEntity => String = obj =>
        toJsonapi(obj).asJson.noSpaces

    override val decodeHandler: String => channelEntity = str =>
        formJsonapi[PhMaxJob](decodeJson[RootObject](parseJson(str)))

    override val consumeHandler: String => Unit = input => {
        val a = decodeHandler(input)
        println(a)

//        val act = context.actorOf(xmppFactor.props(this))
//        val r = act ? decodeHandler(input)
//        Await.result(r.mapTo[channelEntity], t.duration)
    }
}
