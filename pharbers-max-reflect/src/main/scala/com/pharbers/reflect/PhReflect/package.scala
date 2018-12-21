package com.pharbers.reflect

import java.io.File
import java.net.URLClassLoader

import akka.actor.ActorSelection
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionTrait}
import com.pharbers.reflect.PhEntity.confTrait.PhActionTrait

package object PhReflect {

    /** 反射为action */
    def reflect(action: PhActionTrait)(initArgs: Map[String, String])
               (implicit sender: ActorSelection): pActionTrait = {
        if (action.jar_path == "")
            reflectClazz(action.clazz, initArgs)(sender)
        else
            reflectClazzByJar(action.jar_path, action.clazz, initArgs)(sender)
    }

    /** action执行 */
    implicit class pActionExec(action: pActionTrait) {
        def exec(): String = {
            action.perform(MapArgs(Map()))
                    .asInstanceOf[MapArgs].get("result")
                    .asInstanceOf[StringArgs].get
        }
    }

    def reflectClazz(clazz: String, initArgs: Map[String, String])
                    (implicit sender: ActorSelection): pActionTrait = {
        val constructor = Class.forName(clazz).getConstructors()(0)
        constructor.newInstance(initArgs, sender).asInstanceOf[pActionTrait]
    }

    def reflectClazzByJar(jar_path: String, clazz: String, initArgs: Map[String, String])
                         (implicit sender: ActorSelection): pActionTrait = {
        val url = new File(jar_path).toURI.toURL
        val loader = new URLClassLoader(Array(url))
        val constructor = loader.loadClass(clazz).getConstructors()(0)
        constructor.newInstance(initArgs, sender).asInstanceOf[pActionTrait]
    }
}
