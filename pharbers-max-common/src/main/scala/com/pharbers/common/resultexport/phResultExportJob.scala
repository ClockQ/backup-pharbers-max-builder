package com.pharbers.common.resultexport

import com.pharbers.pactions.actionbase._
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.common.action.phResult2StringJob
import com.pharbers.pactions.generalactions.setLogLevelAction
import org.apache.spark.listener.addListenerAction
import org.apache.spark.listener.sendProgress.sendXmppMultiProgress

object phResultExportJob {
    def apply(args: Map[String, String])
             (implicit send: channelEntity => Unit): pActionTrait = new phResultExportJob(args)
}

class phResultExportJob(args: Map[String, String])
                       (implicit send: channelEntity => Unit) extends sequenceJobWithMap {
    override val name: String = "phExportMaxResultJob"

    val max_path: String = args("max_path")
    val max_name: String = args("max_name")
    val max_delimiter: String = args.getOrElse("max_delimiter", 31.toChar.toString)
    val export_path: String = args("export_path")
    val export_name: String = args("export_name")
    val export_delimiter: String = args.getOrElse("export_delimiter", 31.toChar.toString)

    val ym: String = args.getOrElse("ym", "")
    val mkt: String = args.getOrElse("mkt", "")
    val job_id: String = args("job_id")
    val user_id: String = args("user_id")
    val company_id: String = args("company_id")
    val p_current: Double = args("p_current").toDouble
    val p_total: Double = args("p_total").toDouble

    val df = MapArgs(
        Map(
            "ym" -> StringArgs(ym),
            "mkt" -> StringArgs(mkt),
            "job_id" -> StringArgs(job_id),
            "user_id" -> StringArgs(user_id),
            "company_id" -> StringArgs(company_id),
            "max_path" -> StringArgs(max_path),
            "max_name" -> StringArgs(max_name),
            "max_delimiter" -> StringArgs(max_delimiter),
            "export_path" -> StringArgs(export_path),
            "export_name" -> StringArgs(export_name),
            "export_delimiter" -> StringArgs(export_delimiter)
        )
    )

    val tranFun: SingleArgFuncArgs[pActionArgs, StringArgs] = phResult2StringJob.str2StrTranFun
    implicit val xp: Map[String, Any] => Unit = sendXmppMultiProgress(company_id, user_id, "resultExport", job_id)(p_current, p_total).multiProgress

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                addListenerAction(1, 30, job_id) ::
                phLoadMaxResultAction(df) ::
                addListenerAction(41, 90, job_id) ::
                phExportMaxResultAction(df) ::
                addListenerAction(91, 99, job_id) ::
                phResult2StringJob("phExportMaxResultAction", tranFun) ::
                Nil
    }
}

