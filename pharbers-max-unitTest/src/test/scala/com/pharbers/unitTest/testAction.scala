package com.pharbers.unitTest

import akka.actor.ActorSystem
import com.pharbers.pactions.actionbase.MapArgs
import com.pharbers.unitTest.action.PhActionArgs
import com.pharbers.unitTest.common.getSingleAction
import com.pharbers.reflect.util.generateNameAction._

object testAction extends App {
    implicit val as: ActorSystem = ActorSystem("maxActor")

//    val actionLst = generateNameAction("max_json/astellas-all-1804.json", "max_json/astellas-all-1805.json").toSingleList
    val actionLst = generateNameAction("max_json/nhwa-mz-1804.json", "max_json/nhwa-mz-1805.json").toSingleList

    println("市场共 :" + actionLst.length)
    actionLst.foreach { action =>
        println(action.panelConf.get.head.mkt)
    }

    actionLst.foreach { action =>
        println("开始 :" + action.panelConf.get.head.mkt)
        resultCheckJob(PhActionArgs(action))(as).perform(MapArgs(Map()))
    }
}