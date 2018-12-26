package com.pharbers.unitTest

import akka.actor.ActorSystem
import com.pharbers.pactions.actionbase.MapArgs
import com.pharbers.unitTest.action.PhActionArgs
import com.pharbers.unitTest.common.getSingleAction
import com.pharbers.reflect.util.generateNameAction._

object testAction extends App {
    implicit val as: ActorSystem = ActorSystem("maxActor")

    val actionLst = generateNameAction("max_json/nhwa-mz-1804.json", "max_json/tmp-nhwa-mz-1804.json").toSingleList
//    val actionLst = generateNameAction("max_json/astellas-all-1804.json", "max_json/tmp-astellas-all-1804.json").toSingleList
//    val actionLst = generateNameAction("max_json/tq-rp-1806.json", "max_json/tmp-tq-rp-1806.json").toSingleList
//    val actionLst = generateNameAction("max_json/tq-sa-1806.json", "max_json/tmp-tq-sa-1806.json").toSingleList

    println("市场共 :" + actionLst.length)
    actionLst.foreach { action =>
        println(action.unitTestConf.get.head.mkt)
    }

    actionLst.foreach { action =>
        println("开始 :" + action.unitTestConf.get.head.mkt)
        resultCheckJob(PhActionArgs(action))(as).perform(MapArgs(Map()))
    }
}