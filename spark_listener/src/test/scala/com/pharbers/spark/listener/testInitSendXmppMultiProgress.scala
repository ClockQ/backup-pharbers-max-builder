package com.pharbers.spark.listener

import com.pharbers.channel.detail.channelEntity
import com.pharbers.spark.listener.sendProgress.sendXmppMultiProgress

/**
  * @description:
  * @author: clock
  * @date: 2019-04-09 10:48
  */
object testInitSendXmppMultiProgress extends App {
    implicit val send: channelEntity => Unit = { _ => Unit}
    implicit val xp: Map[String, Any] => Unit =
        sendXmppMultiProgress("company_id", "user_id", "panel", "job_id")(1, 2).multiProgress
}
