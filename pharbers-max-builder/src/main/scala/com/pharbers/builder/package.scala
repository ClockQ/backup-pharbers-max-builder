package com.pharbers

import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.builder.PhEntity.PhXmppConf
import com.pharbers.channel.driver.xmpp.xmppClient
import com.pharbers.channel.consumer.callJobXmppConsumer
import com.pharbers.builder.PhReflect.{reflectClazz, reflectClazzByJar}
import com.pharbers.builder.PhEntity.confTrait.{PhConfTrait, PhActionTrait}
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionTrait}

package object builder {

    /** 启动XMPP */
    def startXMPP(conf: Option[PhConfTrait])(as: ActorSystem): ActorSelection = {
        val xmppconf = conf.getOrElse(throw new Exception("xmpp conf is none")).asInstanceOf[PhXmppConf]

        val lactor = xmppClient.startLocalClient(
            new callJobXmppConsumer()(as, xmppconf.conf)
        )(as, xmppconf.conf)

        if (xmppconf.disableSend) as.actorSelection("akka://maxActor/user/null")
        else as.actorSelection(lactor)
    }

    /** 停止XMPP */
    def stopXMPP(conf: Option[PhConfTrait])(as: ActorSystem): Unit = {
        val xmppconf = conf.getOrElse(throw new Exception("xmpp conf is none")).asInstanceOf[PhXmppConf]
        xmppClient.stopLocalClient()(as, xmppconf.conf)
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
