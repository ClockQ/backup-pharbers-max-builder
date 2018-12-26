package com.pharbers.main.PhProcess

import com.pharbers.reflect.PhReflect._
import com.pharbers.spark.phSparkDriver
import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.channel.detail.channelEntity
import com.pharbers.reflect.PhEntity.PhActionJob
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.main.PhConsumer.mainXmppConfBase
import com.pharbers.reflect.PhEntity.confEntity.PhXmppConf
import org.apache.spark.listener.entity.PhMaxJobResult

case class PhBuilder(actionJob: PhActionJob)(implicit as: ActorSystem) {
    val xmppconf: PhXmppConf = actionJob.xmppConf.getOrElse(throw new Exception("xmpp conf is none"))

    val sendActor: ActorSelection = {
        if (xmppconf.disableSend)
            as.actorSelection(xmppFactor.getNullActor(as))
        else
            as.actorSelection(s"akka://${as.name}/user/${mainXmppConfBase.xmpp_user}")
    }

    val sender: channelEntity => Unit = obj => sendActor ! (xmppconf.xmpp_report, obj)

    /** 停止 Spark */
    def stopSpark(): PhBuilder = {
        phSparkDriver(actionJob.job_id).stopCurrConn()
        this
    }

    /** 执行 月份筛选 */
    def calcYmExec(): PhBuilder = {
        actionJob.calcYmConf match {
            case Some(calcYmCnf) =>
                val ymlst = reflect(calcYmCnf)(actionJob.ymArgs(calcYmCnf))(sender).exec()
                println(ymlst)
                val result = new PhMaxJobResult
                result.company_id = actionJob.company_id
                result.user_id = actionJob.user_id
                result.call = "ymCalc"
                result.job_id = actionJob.job_id
                result.percentage = 100
                result.message = ymlst
                sender(result)
                this
            case None => this
        }
    }

    /** 执行 Panel */
    def panelExec(): PhBuilder = {
        actionJob.panelConf match {
            case Some(panelActionLst) =>
                val length = panelActionLst.length
                var currentJobIndex = 0
                val panelLst = panelActionLst.map { panelConf =>
                    currentJobIndex += 1
                    reflect(panelConf)(actionJob.panelArgs(currentJobIndex, length)(panelConf))(sender).exec()
                }
                println(panelLst.mkString("#"))
                val result = new PhMaxJobResult
                result.company_id = actionJob.company_id
                result.user_id = actionJob.user_id
                result.call = "panel"
                result.job_id = actionJob.job_id
                result.percentage = 100
                result.message = panelLst.mkString("#")
                sender(result)
                this
            case None => this
        }
    }

    /** 执行 Calc Max */
    def calcExec(): PhBuilder = {
        actionJob.calcConf match {
            case Some(calcActionLst) =>
                val length = calcActionLst.length
                var currentJobIndex = 0
                val calcLst = calcActionLst.map { calcConf =>
                    currentJobIndex += 1
                    reflect(calcConf)(actionJob.calcArgs(currentJobIndex, length)(calcConf))(sender).exec()
                }
                println(calcLst.mkString("#"))
                val result = new PhMaxJobResult
                result.company_id = actionJob.company_id
                result.user_id = actionJob.user_id
                result.call = "calc"
                result.job_id = actionJob.job_id
                result.percentage = 100
                result.message = calcLst.mkString("#")
                sender(result)
                this
            case None => this
        }
    }
}
