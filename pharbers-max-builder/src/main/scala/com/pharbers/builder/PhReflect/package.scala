package com.pharbers.builder

import java.io.File
import java.net.URLClassLoader
import akka.actor.ActorSelection
import com.pharbers.pactions.actionbase.pActionTrait

package object PhReflect {

    def reflectClazz(clazz: String, initArgs: Map[String, String])(implicit lactor: ActorSelection): pActionTrait = {
        val constructor = Class.forName(clazz).getConstructors()(0)
        constructor.newInstance(initArgs, lactor).asInstanceOf[pActionTrait]
    }

    def reflectClazzByJar(jar_path: String, clazz: String, initArgs: Map[String, String])(implicit lactor: ActorSelection): pActionTrait = {
        val url = new File(jar_path).toURI.toURL
        val loader = new URLClassLoader(Array(url))
        val constructor = loader.loadClass(clazz).getConstructors()(0)
        constructor.newInstance(initArgs, lactor).asInstanceOf[pActionTrait]
    }
}
