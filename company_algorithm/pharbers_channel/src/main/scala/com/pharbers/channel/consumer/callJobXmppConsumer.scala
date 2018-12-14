package com.pharbers.channel.consumer

import akka.actor.ActorSystem
import akka.util.Timeout
import com.pharbers.jsonapi.model._
import com.pharbers.macros._
import io.circe.syntax._
import akka.pattern.ask
import scala.concurrent.Await
import scala.language.postfixOps
import scala.concurrent.duration._
import com.pharbers.channel.detail.{PhMaxJob, channelEntity}
import com.pharbers.channel.driver.xmpp.xmppClient
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppTrait
import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
import com.pharbers.macros.convert.jsonapi.JsonapiMacro._

class callJobXmppConsumer(context : ActorSystem) extends xmppTrait with CirceJsonapiSupport {
    implicit val t: Timeout = 5 hours

    override val encodeHandler: channelEntity => String = obj =>
        toJsonapi(obj).asJson.noSpaces

    override val decodeHandler: String => channelEntity = str =>
        formJsonapi[PhMaxJob](decodeJson[RootObject](parseJson(str)))

    override val consumeHandler: String => Unit = input => {
        val act = context.actorOf(xmppClient.props(this))
        val r = act ? decodeHandler(input)
        Await.result(r.mapTo[channelEntity], t.duration)
    }
}
