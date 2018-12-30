package com.pharbers.main.PhHelper

import com.pharbers.ErrorCode._
import akka.actor.{Actor, ActorSystem, Props}
import com.pharbers.main.PhProcess.PhBuilder
import com.pharbers.reflect.PhEntity.{PhActionError, PhActionJob}

object doJobActor {
    def props(implicit as: ActorSystem) = Props(new doJobActor)
}

class doJobActor(implicit as: ActorSystem) extends Actor {
    override def receive: Receive = {
        case msg: PhActionJob =>
            val builder = PhBuilder(msg)
            try{
                builder.calcYmExec().panelExec().calcExec().exportExec().stopSpark()
            }catch{
                case ex: Exception =>
                    ex.printStackTrace()
                    builder.sender(PhActionError(
                        status = "500",
                        code = getErrorCodeByName(ex.getMessage).toString,
                        title = ex.getMessage,
                        detail = getErrorMessageByName(ex.getMessage)
                    ))
                    builder.stopSpark()
            }
        case _ => ???
    }
}
