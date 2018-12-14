package com.pharbers.panel.nhwa

import java.util.UUID

import akka.actor.Actor
import com.pharbers.channel.util.sendEmTrait
import com.pharbers.pactions.generalactions._
import com.pharbers.panel.common.phCalcYM2JVJob
import com.pharbers.common.algorithm.max_path_obj
import com.pharbers.pactions.jobs.{sequenceJob, sequenceJobWithMap}
import com.pharbers.pactions.actionbase.{StringArgs, pActionArgs, pActionTrait}
import org.apache.spark.listener
import org.apache.spark.listener.addListenerAction
import org.apache.spark.listener.progress.sendXmppSingleProgress

case class phNhwaCalcYMJob(args: Map[String, String])(implicit _actor: Actor) extends sequenceJobWithMap {
    override val name: String = "phNhwaCalcYMJob"
    lazy val cache_location: String = max_path_obj.p_cachePath + UUID.randomUUID().toString
    lazy val cpa_file: String = max_path_obj.p_clientPath + args("cpa")

    lazy val user_id: String = args("user_id")
    lazy val company_id: String = args("company_id")
    lazy val job_id: String = args("job_id")
    implicit val xp: (sendEmTrait, Double, String) => Unit = sendXmppSingleProgress(company_id, user_id, "ymCalc", job_id).singleProgress

    val readCpa: sequenceJob = new sequenceJob {
        override val name = "cpa"
        override val actions: List[pActionTrait] = readCsvAction(cpa_file, applicationName = job_id) :: Nil
    }

    val jobArgs: StringArgs = StringArgs(job_id)

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
            addListenerAction(listener.MaxSparkListener(0, 99, job_id), job_id) ::
            //            addListenerAction(listener.MaxSparkListener(0, 35, job_id), job_id) ::
            readCpa ::
            //            addListenerAction(listener.MaxSparkListener(36, 65, job_id), job_id) ::
            phNhwaCalcYMConcretJob(jobArgs) ::
            //            addListenerAction(listener.MaxSparkListener(66, 95, job_id), job_id) ::
            phCalcYM2JVJob(jobArgs) ::
            Nil
    }
}