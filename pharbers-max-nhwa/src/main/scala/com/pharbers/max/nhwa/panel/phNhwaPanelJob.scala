package com.pharbers.max.nhwa.panel

import com.pharbers.pactions.jobs._
import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase._

case class phNhwaPanelJob(args: Map[String, String])
                         (implicit send: MapArgs => Unit,
                          sparkDriver: phSparkDriver) extends sequenceJobWithMap {
    override val name: String = "phNhwaPanelJob"

    import com.pharbers.pactions.generalactions._

    val ym: String = args("ym")
    val mkt: String = args("mkt")
    val job_id: String = args("job_id")
    val user_id: String = args("user_id")
    val company_id: String = args("company_id")
    val prod_name_lst: List[String] = args("prod_name_lst").split("#").toList
    val p_current: Double = args.getOrElse("p_current", "1").toDouble
    val p_total: Double = args.getOrElse("p_total", "1").toDouble

    val cpa_file: String = args("cpa_file")
    val miss_hosp_file: String = args("miss_hosp_file")
    val not_publish_hosp_file: String = args("not_publish_hosp_file")
    val full_hosp_file: String = args("full_hosp_file")
    val sample_hosp_file: String = args("sample_hosp_file")

    val panel_path: String = args("panel_path")
    val panel_name: String = args("panel_name")

    val readCpa = readParquetAction(cpa_file, "cpa_data")
    val readMissHosp = readParquetAction(miss_hosp_file, "miss_hosp_data")
    val readNotPublishHosp = readParquetAction(not_publish_hosp_file, "not_publish_hosp_data")
    val readFullHosp = readParquetAction(full_hosp_file, "full_hosp_data")
    val readSampleHosp = readParquetAction(sample_hosp_file, "sample_hosp_data")

    val df = MapArgs(
        Map(
            "ym" -> StringArgs(ym)
            , "mkt" -> StringArgs(mkt)
            , "job_id" -> StringArgs(job_id)
            , "company_id" -> StringArgs(company_id)
            , "panel_path" -> StringArgs(panel_path)
            , "panel_name" -> StringArgs(panel_name)
            , "prod_name" -> ListArgs(prod_name_lst.map(StringArgs))
        )
    )

    import com.pharbers.max.common.action.packProgress
    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR") ::
                sendProgressAction(1) ::
                readCpa ::
                sendProgressAction(11) ::
                readMissHosp ::
                sendProgressAction(21) ::
                readNotPublishHosp ::
                sendProgressAction(31) ::
                readFullHosp ::
                sendProgressAction(41) ::
                readSampleHosp ::
                sendProgressAction(51) ::
                phNhwaPanelConcretAction(df) ::
                sendProgressAction(81) ::
//                phSavePanelAction(df, "phNhwaCleanConcretAction", "phSaveCleanAction") ::
                sendProgressAction(91) ::
//                phResult2StringAction("phSaveCleanAction", phResult2StringAction.str2StrTranFun) ::
                Nil
    }

}