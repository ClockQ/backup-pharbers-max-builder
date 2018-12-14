package com.pharbers.channel.progress

import akka.actor.ActorSelection
import com.pharbers.channel.detail.channelEntity

trait sendProgressTrait {
    def sendProcess(obj: channelEntity)(implicit actorRef: ActorSelection): Unit
}