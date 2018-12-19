package com.pharbers.channel.send

import java.util.Date
import play.api.libs.json.JsValue
import play.api.libs.json.Json.toJson
import com.pharbers.channel.driver.em.emDriver

/**
  * Created by clock on 18-12-13.
  */
trait sendTraitByEm extends sendTrait {
    val ed: emDriver = emDriver()

    def sendMessage(args: Map[String, Any]): Unit = {
        val callJob: String = args("callJob").asInstanceOf[String]
        val jobStage: String = args("jobStage").asInstanceOf[String]
        val targetUser: String = args("targetUser").asInstanceOf[String]
        val attributes: JsValue = args("attributes").asInstanceOf[JsValue]
        val targetGroup: String = args("targetGroup").asInstanceOf[String]

        val msg = toJson(toJson(
            Map(
                "messageType" -> toJson("application/json"), // 消息类型
                "date" -> toJson(new Date().getTime.toString), //时间，发送消息是的时间戳
                "call" -> toJson(callJob), //job 类型
                "stage" -> toJson(jobStage), //标记消息状态，开始start、进行中ing、错误error、结束done
                "target" -> toJson(targetUser), //群组内那个人处理
                "attributes" -> attributes
            )
        ).toString)

        ed.sendMessage2Group(targetGroup, msg)
    }

    def sendError(args: Map[String, Any]): Unit = {
        val callJob: String = args("callJob").asInstanceOf[String]
        val targetUser: String = args("targetUser").asInstanceOf[String]
        val errorMsg: JsValue = args("errorMsg").asInstanceOf[JsValue]
        val targetGroup: String = args("targetGroup").asInstanceOf[String]

        val msg = toJson(toJson(
            Map(
                "messageType" -> toJson("application/json"), // 消息类型
                "date" -> toJson(new Date().getTime.toString), //时间，发送消息是的时间戳
                "call" -> toJson(callJob), //job 类型
                "stage" -> toJson("error"), //标记消息状态，开始start、进行中ing、错误error、结束done
                "target" -> toJson(targetUser), //群组内那个人处理
                "attributes" -> toJson(Map("progress" -> toJson("0"))),
                "error" -> errorMsg
            )
        ).toString)

        ed.sendMessage2Group(targetGroup, msg)
    }
}
