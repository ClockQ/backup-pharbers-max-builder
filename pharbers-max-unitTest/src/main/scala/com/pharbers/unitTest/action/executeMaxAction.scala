package com.pharbers.unitTest.action

import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.channel.driver.xmpp.xmppFactor
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

        implicit val sender: ActorSelection = as.actorSelection(xmppFactor.getNullActor(as))

        // 执行Panel
        val panelConf = action.panelConf.get.head
        reflect(panelConf)(action.panelArgs(1, 1)(panelConf)).exec()

        // 执行Max
        val maxConf = action.calcConf.get.head
        reflect(maxConf)(action.calcArgs(1, 1)(maxConf)).exec()

        StringArgs(maxConf.max_name)
    }
}
