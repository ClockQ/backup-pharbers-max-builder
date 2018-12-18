package com.pharbers.nhwa.calc

import akka.actor.ActorSelection
import com.pharbers.pactions.jobs._
import com.pharbers.pactions.actionbase._
import com.pharbers.nhwa.phResult2StringJob
import com.pharbers.pactions.generalactions._
import org.apache.spark.listener.addListenerAction
import org.apache.spark.listener.sendProgress.sendXmppMultiProgress

case class phMaxJob(args: Map[String, String])(implicit sendActor: ActorSelection) extends sequenceJobWithMap {
    override val name: String = "phMaxCalcJob"

    val panel_path: String = args("panel_path")
    val panel_name: String = args("panel_name")
    val max_path: String = args("max_path")
    val max_name: String = args("max_name")
    val max_search_name: String = args("max_search_name")
    val panel_file: String = panel_path + panel_name
    val universe_file: String = args("universe_file")
    lazy val prod_name_lst: List[String] = args("prod_lst").split("#").toList

    lazy val ym: String = args("ym")
    lazy val mkt: String = args("mkt")
    lazy val user_id: String = args("user_id")
    lazy val job_id: String = args("job_id")
    lazy val company_id: String = args("company_id")
    lazy val p_current: Double = args("p_current").toDouble
    lazy val p_total: Double = args("p_total").toDouble

    // 1. load panel data
    val loadPanelData: sequenceJob = new sequenceJob {
        override val name: String = "panel_data"
        override val actions: List[pActionTrait] = readCsvAction(panel_file, delimiter = 31.toChar.toString, applicationName = job_id) :: Nil
    }

    // 留做测试 1. load panel data of xlsx
    //    val loadPanelDataOfExcel: sequenceJob = new sequenceJob {
    //        val temp_panel_name: String = UUID.randomUUID().toString
    //        override val name = "panel_data"
    //        override val actions: List[pActionTrait] =
    //            xlsxReadingAction[PhExcelXLSXCommonFormat](panel_file, temp_panel_name) ::
    //                    saveCurrenResultAction(temp_dir + temp_panel_name) ::
    //                    csv2DFAction(temp_dir + temp_panel_name) :: Nil
    //    }

    // 2. read universe file
    val readUniverseFile: sequenceJob = new sequenceJob {
        override val name = "universe_data"
        override val actions: List[pActionTrait] = readCsvAction(universe_file, applicationName = job_id) :: Nil
    }

    val df = MapArgs(
        Map(
            "ym" -> StringArgs(ym),
            "mkt" -> StringArgs(mkt),
            "user_id" -> StringArgs(user_id),
            "company_id" -> StringArgs(company_id),
            "job_id" -> StringArgs(job_id),
            "panel_name" -> StringArgs(panel_name),
            "max_path" -> StringArgs(max_path),
            "max_name" -> StringArgs(max_name),
            "max_search_name" -> StringArgs(max_search_name),
            "prod_name" -> ListArgs(prod_name_lst.map(StringArgs))
        )
    )

    val tranFun: SingleArgFuncArgs[pActionArgs, StringArgs] = phResult2StringJob.str2StrTranFun
    implicit val xp: Map[String, Any] => Unit = sendXmppMultiProgress(company_id, user_id, "calc", job_id)(p_current, p_total).multiProgress

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                addListenerAction(0, 10, job_id) ::
                loadPanelData ::
                readUniverseFile ::
                phMaxCalcAction(df) ::
                addListenerAction(6, 40, job_id) ::
                phMaxPersistentAction(df) ::
                addListenerAction(41, 99, job_id) ::
                phMaxInfo2RedisAction(df) ::
                phResult2StringJob("phMaxInfo2RedisAction", tranFun) ::
                Nil
    }

}