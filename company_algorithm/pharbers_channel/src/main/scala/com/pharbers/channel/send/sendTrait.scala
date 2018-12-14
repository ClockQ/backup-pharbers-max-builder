package com.pharbers.channel.send

/**
  * @ ProjectName pharbers-max.com.pharbers.channel.util.sendTrait
  * @ author jeorch
  * @ date 18-10-8
  * @ Description: TODO
  */
trait sendTrait {
    def sendMessage(args: Map[String, Any]): Unit
    def sendError(args: Map[String, Any]): Unit
}
