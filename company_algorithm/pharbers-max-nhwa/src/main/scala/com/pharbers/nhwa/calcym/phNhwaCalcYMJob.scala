package com.pharbers.nhwa.calcym

import akka.actor.ActorSelection
import com.pharbers.pactions.generalactions._
import org.apache.spark.listener.helper.ListenerHelper
import com.pharbers.pactions.actionbase.{StringArgs, pActionTrait}
import com.pharbers.pactions.jobs.{sequenceJob, sequenceJobWithMap}
import org.apache.spark.listener.sendProgress.sendXmppSingleProgress
import org.apache.spark.listener.{MaxSparkListener, addListenerAction}

case class phNhwaCalcYMJob(args: Map[String, String])(implicit sendActor: ActorSelection) extends sequenceJobWithMap {
    override val name: String = "phNhwaCalcYMJob"

    lazy val cpa_file: String = args("cpa_file")
    lazy val user_id: String = args("user_id")
    lazy val company_id: String = args("company_id")
    lazy val job_id: String = args("job_id")

    val readCpa: sequenceJob = new sequenceJob {
        override val name = "cpa"
        override val actions: List[pActionTrait] = readCsvAction(arg_path = cpa_file, applicationName = job_id) :: Nil
    }

    val jobArgs: StringArgs = StringArgs(job_id)

    implicit val xp: Map[String, Any] => Unit = sendXmppSingleProgress(company_id, user_id, "ymCalc", job_id).singleProgress
    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
            addListenerAction(MaxSparkListener(ListenerHelper(0, 99), job_id)) ::
            readCpa ::
            phNhwaCalcYMConcretJob(jobArgs) ::
            phCalcYM2JVJob(jobArgs) ::
            Nil
    }
}