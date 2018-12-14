package com.pharbers.channel.util

import play.api.libs.json.JsValue

/**
  * @ ProjectName pharbers-max.com.pharbers.channel.util.sendTrait
  * @ author jeorch
  * @ date 18-10-8
  * @ Description: TODO
  */
trait sendTrait {

    def sendMessage(targetGroup: String, targetUser: String, callJob: String,
                    jobStage: String, attributes: JsValue): Unit

    def sendError(targetGroup: String, targetUser: String, callJob: String, errorMsg: JsValue): Unit

}
