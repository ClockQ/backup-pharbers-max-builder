package com.pharbers.panel.pfizer

import java.util.UUID

import akka.actor.Actor
import com.pharbers.channel.util.sendEmTrait
import com.pharbers.pactions.generalactions._
import com.pharbers.common.algorithm.max_path_obj
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionTrait}
import com.pharbers.pactions.jobs.{sequenceJob, sequenceJobWithMap}
import com.pharbers.panel.common.phCalcYM2JVJobWithCpaAndGyc
import org.apache.spark.listener.progress.{sendSingleProgress, sendXmppSingleProgress}
import com.pharbers.panel.pfizer.format.{phPfizerCpaFormat, phPfizerGycxFormat}
import org.apache.spark.listener.{MaxSparkListener, addListenerAction}

/**
  * Created by jeorch on 18-4-18.
  */
case class phPfizerCalcYMJob(args: Map[String, String])(implicit _actor: Actor) extends sequenceJobWithMap {
    override val name: String = "phPfizerCalcYMJob"
    lazy val cpa_file: String = max_path_obj.p_clientPath + args("cpa")
    lazy val gyc_file: String = max_path_obj.p_clientPath + args("gycx")
    lazy val cache_location: String = max_path_obj.p_cachePath + UUID.randomUUID().toString
    
    lazy val user_id: String = args("user_id")
    lazy val company_id: String = args("company_id")
    lazy val job_id: String = args("job_id")
//    implicit val sp: (sendEmTrait, Double, String) => Unit = sendSingleProgress(company_id, user_id).singleProgress
    implicit val xp: (sendEmTrait, Double, String) => Unit = sendXmppSingleProgress(company_id, user_id, "ymCalc", job_id).singleProgress

    val readCpa: sequenceJob = new sequenceJob {
        override val name = "cpa"
        override val actions: List[pActionTrait] =
            readCsvAction(cpa_file, applicationName = job_id) :: Nil
    }

    val readGyc: sequenceJob = new sequenceJob {
        override val name = "gyc"
        override val actions: List[pActionTrait] =
            readCsvAction(gyc_file, applicationName = job_id) :: Nil
    }

    val df = MapArgs(
        Map(
            "job_id" -> StringArgs(job_id)
        )
    )

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                readCpa ::
                readGyc ::
//                addListenerAction(MaxSparkListener(0, 99, job_id), job_id) ::
                addListenerAction(MaxSparkListener(0, 50, job_id), job_id) ::
                phPfizerCalcYMCpaConcretJob(df) ::
                phPfizerCalcYMGycxConcretJob(df) ::
//                saveMapResultAction("calcYMWithCpa", cache_location + "cpa") ::
//                saveMapResultAction("calcYMWithGycx", cache_location + "gycx") ::
                addListenerAction(MaxSparkListener(51, 99, job_id), job_id) ::
                phCalcYM2JVJobWithCpaAndGyc(df) ::
                Nil
    }
}