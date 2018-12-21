package com.pharbers.unitTest.action

import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.channel.consumer.commonXmppConsumer
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionArgs, pActionTrait}

object executeMaxAction {
    def apply(args: pActionArgs)(implicit as: ActorSystem): pActionTrait = new executeMaxAction(args)
}

class executeMaxAction(override val defaultArgs: pActionArgs)
                      (implicit as: ActorSystem) extends pActionTrait {

    import com.pharbers.reflect.PhReflect._

    override val name: String = "max_result"

    override def perform(prMap: pActionArgs): pActionArgs = {
        val action = prMap.asInstanceOf[MapArgs].get("generateName").asInstanceOf[PhActionArgs].get

        val xmppconf = action.xmppConf.get
        xmppFactor.startLocalClient(new commonXmppConsumer())(as, xmppconf.conf)
        implicit val sender: ActorSelection = as.actorSelection(xmppFactor.getNullActor(as))

        // 执行Panel
        val panelConf = action.panelConf.get.head
        reflect(panelConf)(action.panelArgs(1, 1)(panelConf)).exec()

        // 执行Max
        val maxConf = action.calcConf.get.head
        val maxResult = reflect(maxConf)(action.calcArgs(1, 1)(maxConf)).exec()
        println(maxResult)
        StringArgs(maxResult)
    }
}
