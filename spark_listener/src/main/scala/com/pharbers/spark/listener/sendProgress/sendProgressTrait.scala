package com.pharbers.spark.listener.sendProgress

import com.pharbers.channel.detail.channelEntity

trait sendProgressTrait {
    def sendProcess(obj: channelEntity)(implicit send: channelEntity => Unit): Unit
}