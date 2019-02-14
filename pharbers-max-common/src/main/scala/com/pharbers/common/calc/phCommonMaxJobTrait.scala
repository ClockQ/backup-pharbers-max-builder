package com.pharbers.common.calc

import com.pharbers.pactions.actionbase._
import com.pharbers.channel.detail.channelEntity
import org.apache.spark.listener.addListenerAction
import com.pharbers.common.action.phResult2StringJob
import org.apache.spark.listener.sendProgress.sendXmppMultiProgress
import com.pharbers.pactions.jobs.{sequenceJob, sequenceJobWithMap}
import com.pharbers.pactions.generalactions.{readCsvAction, readParquetAction, setLogLevelAction}

trait phCommonMaxJobTrait extends sequenceJobWithMap {

    override val name: String = "phMaxCalcJob"

    val args: Map[String, String]
    implicit val send: channelEntity => Unit

    val panel_path: String = args("panel_path")
    val panel_name: String = args("panel_name")
    val max_path: String = args("max_path")
    val max_name: String = args("max_name")
    val max_search_name: String = args("max_search_name")
    val panel_delimiter: String = args.getOrElse("panel_delimiter", 31.toChar.toString)
    val max_delimiter: String = args.getOrElse("max_delimiter", 31.toChar.toString)
    val panel_file: String = panel_path + panel_name
    val universe_file: String = args("universe_file")
    val prod_name_lst: List[String] = args("prod_lst").split("#").toList

    val ym: String = args("ym")
    val mkt: String = args("mkt")
    val user_id: String = args("user_id")
    val job_id: String = args("job_id")
    val company_id: String = args("company_id")
    val p_current: Double = args("p_current").toDouble
    val p_total: Double = args("p_total").toDouble

    // 1. load panel data
    lazy val loadPanelData: sequenceJob = new sequenceJob {
        override val name: String = "panel_data"
        override val actions: List[pActionTrait] = readParquetAction(panel_file, applicationName = job_id) :: Nil
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
    lazy val readUniverseFile: sequenceJob = new sequenceJob {
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
            "max_delimiter" -> StringArgs(max_delimiter),
            "max_search_name" -> StringArgs(max_search_name),
            "prod_name" -> ListArgs(prod_name_lst.map(StringArgs))
        )
    )

    val tranFun: SingleArgFuncArgs[pActionArgs, StringArgs] = phResult2StringJob.str2StrTranFun
    implicit val xp: Map[String, Any] => Unit = sendXmppMultiProgress(company_id, user_id, "calc", job_id)(p_current, p_total).multiProgress

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                addListenerAction(1, 10, job_id) ::
                loadPanelData ::
                addListenerAction(11, 20, job_id) ::
                readUniverseFile ::
                addListenerAction(21, 30, job_id) ::
                phMaxCalcAction(df) ::
                addListenerAction(31, 40, job_id) ::
                phMaxPersistentAction(df) ::
                addListenerAction(41, 90, job_id) ::
                phMaxInfo2RedisAction(df) ::
                addListenerAction(91, 99, job_id) ::
                phResult2StringJob("phMaxInfo2RedisAction", tranFun) ::
                Nil
    }

}
