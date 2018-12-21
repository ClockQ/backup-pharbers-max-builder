package com.pharbers

import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.builder.PhEntity.PhXmppConf
import com.pharbers.builder.PhEntity.confTrait.{PhActionTrait, PhConfTrait}
import com.pharbers.builder.PhReflect.{reflectClazz, reflectClazzByJar}
import com.pharbers.channel.consumer.commonXmppConsumer
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionTrait}

package object builder {

    /** 启动XMPP */
    def startXMPP(conf: Option[PhConfTrait])(as: ActorSystem): ActorSelection = {
        val xmppconf = conf.getOrElse(throw new Exception("xmpp conf is none")).asInstanceOf[PhXmppConf]

        val lactor = xmppFactor.startLocalClient(
            new commonXmppConsumer()(as, xmppconf.conf)
        )(as, xmppconf.conf)

        if (xmppconf.disableSend) as.actorSelection(xmppFactor.getNullActor()(as))
        else as.actorSelection(lactor)
    }

    /** 停止XMPP */
    def stopXMPP(conf: Option[PhConfTrait])(as: ActorSystem): Unit = {
        val xmppconf = conf.getOrElse(throw new Exception("xmpp conf is none")).asInstanceOf[PhXmppConf]
        xmppFactor.stopLocalClient()(as, xmppconf.conf)
    }

    /** 反射为action */
    def reflect(action: PhActionTrait)(initArgs: Map[String, String])
               (implicit lactor: ActorSelection): pActionTrait = {
        if (action.jar_path == "")
            reflectClazz(action.clazz, initArgs)(lactor)
        else
            reflectClazzByJar(action.jar_path, action.clazz, initArgs)(lactor)
    }

    /** action执行 */
    implicit class pActionExec(action: pActionTrait) {
        def exec(): String = {
            action.perform(MapArgs(Map()))
                    .asInstanceOf[MapArgs].get("result")
                    .asInstanceOf[StringArgs].get
        }
    }

}
