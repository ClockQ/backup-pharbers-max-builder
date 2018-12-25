package org.apache.spark.listener.sendProgress

import com.pharbers.channel.detail.channelEntity

trait sendProgressTrait {
    def sendProcess(obj: channelEntity)(implicit send: channelEntity => Unit): Unit
}