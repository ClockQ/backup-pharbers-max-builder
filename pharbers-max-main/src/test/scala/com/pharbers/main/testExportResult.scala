package com.pharbers.main

import java.util.Base64

import akka.actor.ActorSystem
import com.pharbers.driver.PhRedisDriver
import com.pharbers.main.PhProcess.PhBuilder
import com.pharbers.reflect.PhEntity.PhActionJob
import com.pharbers.reflect.PhEntity.confEntity.{PhResultExportConf, PhXmppConf}

/**
  * @description:
  * @author: clock
  * @date: 2019-01-23 14:45
  */
object testExportResult extends App {
    val company_id = "5b028f95ed925c2c705b85ba"
    val job_id = "c372f1b8-2558-8a18-d6ef-d24ac03f431b"
    val clazz = "com.pharbers.common.resultexport.phResultExportJob"

    def generateAction: PhActionJob = {
        val rd = new PhRedisDriver()
        val panelLst = rd.getSetAllValue(job_id)
        val ymAndMkt = panelLst.map{ panel_name =>
            val ym = rd.getMapValue(panel_name, "ym")
            val mkt = rd.getMapValue(panel_name, "mkt")
            (ym, mkt)
        }
        val nameLst = ymAndMkt.map{ yam =>
            val singleJobKey = Base64.getEncoder.encodeToString((company_id + "#" + yam._1 + "#" + yam._2).getBytes())
            val max_result_name = rd.getMapValue(singleJobKey, "max_result_name")
            (max_result_name, yam._1 + "_" + yam._2 + "_" + "maxResult")
        }

        val tmpAction: PhActionJob = new PhActionJob()
        tmpAction.job_id = job_id
        tmpAction.user_id = "user_id"
        tmpAction.company_id = company_id
        tmpAction.max_path = "hdfs:///workData/Max/"
        tmpAction.export_path = s"hdfs:///workData/Export/$job_id/"

        val xmppConf = new PhXmppConf
        xmppConf.disableSend = true
        tmpAction.xmppConf = Some(xmppConf)

        val tmpName = nameLst.map{ name =>
            val exportConf = new PhResultExportConf
            exportConf.max_name = name._1
            exportConf.export_name = name._2
            exportConf.clazz = clazz
            exportConf
        }.toList

        tmpAction.resultExportConf = Some(tmpName)
        tmpAction
    }

    implicit val system: ActorSystem = ActorSystem("maxActor")
    PhBuilder(generateAction).exportExec().stopSpark()








}
