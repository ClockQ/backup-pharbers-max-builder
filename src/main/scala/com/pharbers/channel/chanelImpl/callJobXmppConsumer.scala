package com.pharbers.channel.chanelImpl

import akka.actor.ActorSystem
import akka.util.Timeout
import com.pharbers.channel.{executeJob}
import com.pharbers.pattern2.detail.{PhMaxJob, commonresult}
import com.pharbers.pattern2.entry.DispatchEntry
import com.pharbers.xmpp.xmppTrait
import pattern.manager.SequenceSteps
import io.circe.syntax._
import com.pharbers.macros._
import com.pharbers.jsonapi.model._
import com.pharbers.macros.convert.jsonapi.JsonapiMacro._
import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport

import scala.concurrent.duration._

class callJobXmppConsumer(context : ActorSystem) extends xmppTrait with CirceJsonapiSupport {
    implicit val t: Timeout = 5 hours
    val entry = DispatchEntry()(context)

    override val encodeHandler: commonresult => String = obj =>
        toJsonapi(obj).asJson.noSpaces

    override val decodeHandler: String => commonresult = str =>
        formJsonapi[PhMaxJob](decodeJson[RootObject](parseJson(str)))

    override val consumeHandler: String => Unit = input =>
        entry.commonExcution(
            SequenceSteps(executeJob(decodeHandler(input))(context) :: Nil, None))
}
