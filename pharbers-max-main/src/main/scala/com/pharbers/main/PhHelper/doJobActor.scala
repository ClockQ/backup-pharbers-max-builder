package com.pharbers.main.PhHelper

import akka.actor.{Actor, ActorSystem, Props}
import com.pharbers.main.PhProcess.PhBuilder
import com.pharbers.reflect.PhEntity.PhActionJob

object doJobActor {
    def props(implicit as: ActorSystem) = Props(new doJobActor)
}

class doJobActor(implicit as: ActorSystem) extends Actor {
    override def receive: Receive = {
        case msg: PhActionJob => PhBuilder(msg).calcYmExec().panelExec().stopSpark().stopXMPP()//.calcExec()
        case _ => ???
    }
}