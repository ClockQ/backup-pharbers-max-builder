package com.pharbers.pfizer.calc

import com.pharbers.pactions.generalactions._
import com.pharbers.pactions.jobs.sequenceJob
import com.pharbers.channel.detail.channelEntity
import org.apache.spark.listener.addListenerAction
import com.pharbers.common.action.phResult2StringJob
import com.pharbers.pactions.actionbase.pActionTrait
import com.pharbers.pfizer.calc.actions.phMaxCalcActionForDVP
import com.pharbers.common.calc.{phCommonMaxJobTrait, phMaxInfo2RedisAction, phMaxPersistentAction}

/**
  * Created by jeorch on 18-5-3.
  */
case class phPfizerForDVPMaxJob(args: Map[String, String])
                               (override implicit val send: channelEntity => Unit) extends phCommonMaxJobTrait {
    val coef_file: String = args("coef_file")

    // 3. read coef file
    val readCoefFile: sequenceJob = new sequenceJob {
        override val name = "coef_data"
        override val actions: List[pActionTrait] = readCsvAction(coef_file, applicationName = job_id) :: Nil
    }

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
//                addListenerAction(1, 10, job_id) ::
                loadPanelData ::
//                addListenerAction(11, 20, job_id) ::
                readUniverseFile ::
//                addListenerAction(21, 30, job_id) ::
                readCoefFile ::
//                addListenerAction(31, 40, job_id) ::
                phMaxCalcActionForDVP(df) ::
//                addListenerAction(41, 50, job_id) ::
                phMaxPersistentAction(df) ::
//                addListenerAction(51, 90, job_id) ::
                phMaxInfo2RedisAction(df) ::
//                addListenerAction(91, 99, job_id) ::
                phResult2StringJob("phMaxInfo2RedisAction", tranFun) ::
                Nil
    }
}
