package com.pharbers.main.PhProcess

import com.pharbers.reflect.PhReflect._
import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.reflect.PhEntity.PhActionJob
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.main.PhConsumer.progressXmppConsumer
import com.pharbers.spark.phSparkDriver

case class PhBuilder(actionJob: PhActionJob)(implicit as: ActorSystem) {
    lazy val senderActor: ActorSelection = startXMPP()

    /** 启动XMPP */
    private def startXMPP(): ActorSelection = {
        val xmppconf = actionJob.xmppConf.getOrElse(throw new Exception("xmpp conf is none"))

        val lactor = xmppFactor.startLocalClient(
            new progressXmppConsumer()
        )(as, xmppconf.conf)

        if (xmppconf.disableSend) as.actorSelection(xmppFactor.getNullActor(as))
        else as.actorSelection(lactor)
    }

    /** 停止XMPP */
    def stopXMPP(): Unit = {
        Thread.sleep(5000)
        val xmppconf = actionJob.xmppConf.getOrElse(throw new Exception("xmpp conf is none"))
        xmppFactor.stopLocalClient()(as, xmppconf.conf)
    }

    /** 停止XMPP */
    def stopSpark(): PhBuilder = {
        phSparkDriver(actionJob.job_id).stopCurrConn()
        this
    }

    /** 执行 月份筛选 */
    def calcYmExec(): PhBuilder = {
        actionJob.calcYmConf match {
            case Some(calcYmCnf) =>
                reflect(calcYmCnf)(actionJob.ymArgs(calcYmCnf))(senderActor).exec()
                this
            case None => this
        }
    }

    /** 执行 Panel */
    def panelExec(): PhBuilder = {
        actionJob.panelConf match {
            case Some(panelActionLst) =>
                val length = panelActionLst.length
                var currentJobIndex = 1
                panelActionLst.foreach { panelConf =>
                    reflect(panelConf)(actionJob.panelArgs(currentJobIndex, length)(panelConf))(senderActor).exec()
                    currentJobIndex += 1
                }
                this
            case None => this
        }
    }

    /** 执行 Calc Max */
    def calcExec(): PhBuilder = {
        actionJob.calcConf match {
            case Some(calcActionLst) =>
                val length = calcActionLst.length
                var currentJobIndex = 1
                calcActionLst.foreach { calcConf =>
                    reflect(calcConf)(actionJob.calcArgs(currentJobIndex, length)(calcConf))(senderActor).exec()
                    currentJobIndex += 1
                }
                this
            case None => this
        }
    }
}
