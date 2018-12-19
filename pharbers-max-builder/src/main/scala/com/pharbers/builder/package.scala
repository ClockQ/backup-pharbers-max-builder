package com.pharbers

import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.channel.driver.xmpp.xmppClient
import com.pharbers.channel.consumer.callJobXmppConsumer
import com.pharbers.builder.PhReflect.{reflectClazz, reflectClazzByJar}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionTrait}

package object builder {
    def startXMPP(conf: XmppConfigType)(as: ActorSystem): ActorSelection =
        as.actorSelection(xmppClient.startLocalClient(new callJobXmppConsumer()(as, conf))(as, conf))

    def stopXMPP(conf: XmppConfigType)(as: ActorSystem): Unit =
        xmppClient.stopLocalClient()(as, conf)

    def reflect(clazz: String, initArgs: Map[String, String])
               (implicit lactor: ActorSelection): pActionTrait =
        reflectClazz(clazz, initArgs)(lactor)

    def reflect(jar_path: String, clazz: String, initArgs: Map[String, String])
               (implicit lactor: ActorSelection): pActionTrait =
        reflectClazzByJar(jar_path, clazz, initArgs)(lactor)

    implicit class pActionExec(action: pActionTrait){
        def exec(): String = {
            action.perform(MapArgs(Map()))
                    .asInstanceOf[MapArgs].get("result")
                    .asInstanceOf[StringArgs].get
        }
    }
}
