package com.pharbers.servier.calc

import com.pharbers.common.calc._
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJob
import com.pharbers.channel.detail.channelEntity
import org.apache.spark.listener.addListenerAction
import com.pharbers.common.action.phResult2StringJob
import com.pharbers.pactions.generalactions.{readCsvAction, setLogLevelAction}

case class phServierMaxJob(args: Map[String, String])
                          (override implicit val send: channelEntity => Unit) extends phCommonMaxJobTrait {

    override lazy val loadPanelData: sequenceJob = new sequenceJob {
        override val name: String = "panel_data"
        override val actions: List[pActionTrait] =
            readCsvAction(panel_file, delimiter = panel_delimiter, applicationName = job_id) :: Nil
    }

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                addListenerAction(1, 10, job_id) ::
                loadPanelData ::
                addListenerAction(11, 20, job_id) ::
                readUniverseFile ::
                addListenerAction(21, 30, job_id) ::
                phMaxCalcActionByServier(df) ::
                addListenerAction(31, 40, job_id) ::
                phMaxPersistentAction(df) ::
                addListenerAction(41, 90, job_id) ::
                phMaxInfo2RedisAction(df) ::
                addListenerAction(91, 99, job_id) ::
                phResult2StringJob("phMaxInfo2RedisAction", tranFun) ::
                Nil
    }
}
