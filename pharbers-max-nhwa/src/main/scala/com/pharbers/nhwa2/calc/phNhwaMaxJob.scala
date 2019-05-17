package com.pharbers.nhwa2.calc

import com.pharbers.channel.detail.channelEntity
import com.pharbers.common.calc.{phCommonMaxJobTrait, phMaxCalcAction}
import com.pharbers.pactions.actionbase.{ListArgs, MapArgs, StringArgs, pActionTrait}
import com.pharbers.pactions.generalactions.{readCsvAction, readParquetAction, setLogLevelAction}
import com.pharbers.pactions.jobs.{sequenceJob, sequenceJobWithMap}
import com.pharbers.spark.phSparkDriver

case class phNhwaMaxJob(args: Map[String, String])(implicit sparkDriver: phSparkDriver) extends sequenceJobWithMap{

    override val name: String = "phMaxCalcJob"

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
    lazy val loadPanelData = readParquetAction("/test/qi/qi/panel_true", "panel_data")

    // 2. read universe file
    lazy val readUniverseFile = readCsvAction("hdfs:///data/nhwa/pha_config_repository1809/Nhwa_universe_麻醉市场_20180705.csv", ",", "universe_data")

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

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                loadPanelData ::
                readUniverseFile ::
                phMaxCalcAction(df) ::
                Nil
    }
}