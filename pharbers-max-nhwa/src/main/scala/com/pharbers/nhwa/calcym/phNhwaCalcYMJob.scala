package com.pharbers.nhwa.calcym

import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.generalactions._
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.common.action.{phResult2StringJob, readCpa}
import com.pharbers.spark.listener.sendProgress.sendXmppSingleProgress
import org.apache.spark.listener.addListenerAction

case class phNhwaCalcYMJob(args: Map[String, String])(implicit send: channelEntity => Unit) extends sequenceJobWithMap {
    override val name: String = "phNhwaCalcYMJob"

    val cpa_file: String = args("cpa_file")
    val user_id: String = args("user_id")
    val company_id: String = args("company_id")
    val job_id: String = args("job_id")

    val df = MapArgs(
        Map(
            "job_id" -> StringArgs(job_id)
        )
    )

    val tranFun: SingleArgFuncArgs[pActionArgs, StringArgs] = phResult2StringJob.lst2StrTranFun
    implicit val xp: Map[String, Any] => Unit = sendXmppSingleProgress(company_id, user_id, "ymCalc", job_id).singleProgress

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                addListenerAction(1, 10, job_id) ::
                readCpa(cpa_file, job_id) ::
                addListenerAction(31, 90, job_id) ::
                phNhwaCountYm(df) ::
                addListenerAction(91, 99, job_id) ::
                phResult2StringJob("calcYm", tranFun) ::
                Nil
    }
}