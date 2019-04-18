package com.pharbers.unitTest

import akka.actor.ActorSystem
import com.pharbers.pactions.actionbase.MapArgs
import com.pharbers.unitTest.action.PhActionArgs
import com.pharbers.unitTest.common.getSingleAction
import com.pharbers.reflect.util.generateNameAction._

object testAction4 extends App {
    implicit val as: ActorSystem = ActorSystem("maxActor")

    //    val actionLst = generateNameAction("max_json/tmp.json").toSingleList
    //    val actionLst = generateNameAction("max_json/nhwa-mz-1804.json", "max_json/tmp-nhwa-mz-1804.json").toSingleList
    //    val actionLst = generateNameAction("max_json/astellas-all-1804.json", "max_json/tmp-astellas-all-1804.json").toSingleList
    //    val actionLst = generateNameAction("max_json/tq-rp-1806.json", "max_json/tmp-tq-rp-1806.json").toSingleList
    //    val actionLst = generateNameAction("max_json/tq-sa-1806.json", "max_json/tmp-tq-sa-1806.json").toSingleList
    //    val actionLst = generateNameAction("max_json/bayer-all-1811.json", "max_json/tmp-bayer-all-1811.json").toSingleList
    //    val actionLst = generateNameAction("max_json/xlt-all-1811.json", "max_json/tmp-xlt-all-1811.json").toSingleList
    //    val actionLst = generateNameAction("max_json/servier-all-1806.json", "max_json/tmp-servier-all-1806.json").toSingleList
    //    val actionLst = generateNameAction("max_json/pfizer-all-1804.json", "max_json/tmp-pfizer-all-1806.json").toSingleList
    //    val actionLst = generateNameAction("max_json/qilu-all-1809.json", "max_json/tmp-qilu-all-1809.json").toSingleList
    //    val actionLst = generateNameAction("max_json/bt-all-1809.json", "max_json/tmp-bt-all-1809.json").toSingleList
    //    val actionLst = generateNameAction("max_json/BMS-all-1809.json", "max_json/tmp-BMS-all-1809.json").toSingleList

    val actionLst = generateNameAction("max_json/pfizer/17.json", "max_json/pfizer/tmp_17.json").toSingleList

    println("市场共 :" + actionLst.length)
    actionLst.foreach { action =>
        println(action.unitTestConf.get.head.mkt)
    }

    actionLst.foreach { action =>
        println("开始 :" + action.unitTestConf.get.head.mkt)
        resultCheckJob(PhActionArgs(action))(as).perform(MapArgs(Map()))
    }
}