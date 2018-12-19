package com.pharbers.builder

import akka.actor.ActorSystem
import com.pharbers.builder.PhEntity.abcde
import com.pharbers.spark.phSparkDriver

object testReflect extends App {
    val system: ActorSystem = ActorSystem("maxActor")
    val a = new abcde()
    startXMPP(a.xmppConf.xmppconfig)(system)

    val jar_path = "/Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-nhwa/target/"

    val calcYmResult = reflect(
        jar_path = jar_path + "pharbers-max-nhwa-1.0.jar",
        clazz = "com.pharbers.nhwa.calcym.phNhwaCalcYMJob",
        initArgs = a.calcYmConf
    )(system.actorSelection("akka://maxActor/user/null")).exec()
    println(calcYmResult)

    val panelResult = reflect(
        jar_path = jar_path + "pharbers-max-nhwa-1.0.jar",
        clazz = "com.pharbers.nhwa.panel.phNhwaPanelJob",
        initArgs = a.panelConf
    )(system.actorSelection("akka://maxActor/user/null")).exec()
    println(panelResult)

    val calcResult = reflect(
        jar_path = jar_path + "pharbers-max-nhwa-1.0.jar",
        clazz = "com.pharbers.nhwa.calc.phNhwaMaxJob",
        initArgs = a.calcConf
    )(system.actorSelection("akka://maxActor/user/null")).exec()
    println(calcResult)

    phSparkDriver(a.job_id).stopCurrConn
    stopXMPP(a.xmppConf.xmppconfig)(system)
}
