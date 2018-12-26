package com.pharbers.unitTest.action

import akka.actor.ActorSystem
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionArgs, pActionTrait}

object executeMaxAction {
    def apply(args: pActionArgs)(implicit as: ActorSystem): pActionTrait = new executeMaxAction(args)
}

class executeMaxAction(override val defaultArgs: pActionArgs)
                      (implicit as: ActorSystem) extends pActionTrait {

    import com.pharbers.reflect.PhReflect._

    override val name: String = "executeMaxAction"

    override def perform(prMap: pActionArgs): pActionArgs = {
        val action = defaultArgs.asInstanceOf[MapArgs].get("checkAction").asInstanceOf[PhActionArgs].get

        implicit val sender: channelEntity => Unit = {
            _ => Unit
        }

        // 执行Panel
        action.panelConf match {
            case Some(one :: _) => reflect(one)(action.panelArgs(1, 1)(one)).exec()
            case _ => Unit
        }

        // 执行Max
        val maxResult = action.calcConf match {
            case Some(one :: _) =>
                reflect(one)(action.calcArgs(1, 1)(one)).exec()
                one.max_name
            case _ => throw new Exception("no max")
        }

        StringArgs(maxResult)
    }
}
