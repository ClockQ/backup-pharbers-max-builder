package com.pharbers.max.nhwa.clean

import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.max.common.clean.phSaveCleanAction
import com.pharbers.max.common.action.phResult2StringAction

case class phNhwaCleanJob(args: Map[String, String])
                         (implicit send: MapArgs => Unit,
                          sparkDriver: phSparkDriver) extends sequenceJobWithMap {
    override val name: String = "phNhwaCleanJob"

    import com.pharbers.pactions.generalactions._

    val job_id: String = args("job_id")
    val user_id: String = args("user_id")
    val company_id: String = args("company_id")
    val p_current: Double = args.getOrElse("p_current", "1").toDouble
    val p_total: Double = args.getOrElse("p_total", "1").toDouble

    val cpa_file: String = args("cpa_file")
    val hospital_file: String = args("hospital_file")
    val product_file: String = args("product_file")
    val pha_file: String = args("pha_file")
    val product_match_file: String = args("product_match_file")

    val cpa_erd_path: String = args("cpa_erd_path")
    val cpa_erd_name: String = args("cpa_erd_name")

    val readCpa = readCsvAction(cpa_file, ",", "cpa_data")
    val readHosp = readParquetAction(hospital_file, "hospital_data")
    val readProd = readParquetAction(product_file, "product_data")
    val readPha = readParquetAction(pha_file, "pha_data")
    val readProdMatch = readCsvAction(product_match_file, ",", "product_match_data")

    val df = MapArgs(
        Map(
            "job_id" -> StringArgs(job_id)
            , "company_id" -> StringArgs(company_id)
            , "cpa_erd_path" -> StringArgs(cpa_erd_path)
            , "cpa_erd_name" -> StringArgs(cpa_erd_name)
        )
    )

    import com.pharbers.max.common.action.packProgress
    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR") ::
                sendProgressAction(1) ::
                readCpa ::
                sendProgressAction(11) ::
                readHosp ::
                sendProgressAction(21) ::
                readProd ::
                sendProgressAction(31) ::
                readPha ::
                sendProgressAction(41) ::
                readProdMatch ::
                sendProgressAction(51) ::
                phNhwaCleanConcretAction(df) ::
                sendProgressAction(81) ::
                phSaveCleanAction(df, "phNhwaCleanConcretAction", "phSaveCleanAction") ::
                sendProgressAction(91) ::
                phResult2StringAction("phSaveCleanAction", phResult2StringAction.str2StrTranFun) ::
                Nil
    }
}
