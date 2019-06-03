package com.pharbers.max.nhwa.max

import com.pharbers.pactions.jobs._
import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase._

case class phNhwaMaxJob(args: Map[String, String])
                       (implicit send: MapArgs => Unit,
                        sparkDriver: phSparkDriver) extends sequenceJobWithMap {
    override val name: String = "phNhwaMaxJob"

    import com.pharbers.pactions.generalactions._

    val ym: String = args("ym")
    val mkt: String = args("mkt")
    val job_id: String = args("job_id")
    val user_id: String = args("user_id")
    val company_id: String = args("company_id")
    val prod_name_lst: List[String] = args("prod_name_lst").split("#").toList
    val p_current: Double = args.getOrElse("p_current", "1").toDouble
    val p_total: Double = args.getOrElse("p_total", "1").toDouble

    val panel_file: String = args("panel_file")
    val universe_hosp_file: String = args("universe_hosp_file")

    val max_path: String = args("max_path")
    val max_name: String = args("max_name")

    val readPanel = readParquetAction(panel_file, "panel_data")
    val readUniverseHosp = readParquetAction(universe_hosp_file, "universe_hosp_data")

    val df = MapArgs(
        Map(
            "ym" -> StringArgs(ym)
            , "mkt" -> StringArgs(mkt)
            , "job_id" -> StringArgs(job_id)
            , "company_id" -> StringArgs(company_id)
            , "max_path" -> StringArgs(max_path)
            , "max_name" -> StringArgs(max_name)
            , "prod_name" -> ListArgs(prod_name_lst.map(StringArgs))
        )
    )

    import com.pharbers.max.common.action.packProgress

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR") ::
                sendProgressAction(1) ::
                readPanel ::
                sendProgressAction(11) ::
                readUniverseHosp ::
                sendProgressAction(21) ::
                phNhwaMaxConcretAction(df) ::
                sendProgressAction(81) ::
//                phSaveMaxAction(df, "phNhwaCleanConcretAction", "phSaveCleanAction") ::
                sendProgressAction(91) ::
//                phResult2StringAction("phSaveCleanAction", phResult2StringAction.str2StrTranFun) ::
                Nil
    }

}