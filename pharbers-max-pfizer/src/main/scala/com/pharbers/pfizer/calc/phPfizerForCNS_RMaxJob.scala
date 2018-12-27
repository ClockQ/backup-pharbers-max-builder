package com.pharbers.pfizer.calc

import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.generalactions._
import com.pharbers.channel.detail.channelEntity
import org.apache.spark.listener.addListenerAction
import com.pharbers.common.action.phResult2StringJob
import com.pharbers.pfizer.calc.actions.phMaxCalcActionForCNS_R
import com.pharbers.common.calc.{phCommonMaxJobTrait, phMaxInfo2RedisAction, phMaxPersistentAction}

/**
  * Created by jeorch on 18-5-3.
  */
case class phPfizerForCNS_RMaxJob(args: Map[String, String])
                                 (override implicit val send: channelEntity => Unit) extends phCommonMaxJobTrait {
    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                addListenerAction(1, 10, job_id) ::
                loadPanelData ::
                addListenerAction(11, 20, job_id) ::
                readUniverseFile ::
                addListenerAction(21, 30, job_id) ::
                phMaxCalcActionForCNS_R(df) ::
                addListenerAction(31, 40, job_id) ::
                phMaxPersistentAction(df) ::
                addListenerAction(41, 90, job_id) ::
                phMaxInfo2RedisAction(df) ::
                addListenerAction(91, 99, job_id) ::
                phResult2StringJob("phMaxInfo2RedisAction", tranFun) ::
                Nil
    }
}