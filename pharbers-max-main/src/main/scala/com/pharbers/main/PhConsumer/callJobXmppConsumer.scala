package com.pharbers.main.PhConsumer

import io.circe.syntax._
import akka.util.Timeout
import com.pharbers.macros._
import akka.actor.ActorSystem
import scala.language.postfixOps
import scala.concurrent.duration._
import com.pharbers.jsonapi.model._
import com.pharbers.macros.api.errorEntity
import com.pharbers.main.PhHelper.doJobActor
import com.pharbers.channel.detail.channelEntity
import com.pharbers.reflect.PhEntity.PhActionJob
import com.pharbers.macros.convert.jsonapi.JsonapiMacro._
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppTrait
import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
import com.pharbers.main.PhUtil.FilterIDIsNull.FilterIDIsNull

class callJobXmppConsumer()(implicit as: ActorSystem) extends xmppTrait with CirceJsonapiSupport {
    implicit val t: Timeout = 10 seconds

    override val encodeHandler: channelEntity => String = {
        case entity: errorEntity => toErrorsJsonapi(entity).asJson.noSpaces
        case obj => toJsonapi(obj).asJson.noSpaces
    }

    override val decodeHandler: String => channelEntity = str =>
        formJsonapi[PhActionJob](decodeJson[RootObject](parseJson(str)))

    override val consumeHandler: (String, String) => Unit = (_, input) => {
//        val action = generateNameAction(decodeHandler(input).asInstanceOf[PhActionJob])
        val action = decodeHandler(input).asInstanceOf[PhActionJob].filterNullId()
        println("job_id, job_id = " + action.job_id)
        println("company_name, company_name = " + action.prod_lst)
        println("report_user, report_user = " + action.xmppConf.get.xmpp_report)
        val actorRef = as.actorOf(doJobActor.props)
        actorRef ! action
    }
}