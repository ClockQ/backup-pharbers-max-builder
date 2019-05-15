package com.pharbers.nhwa.clean

import com.pharbers.common.action._
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.generalactions._
import com.pharbers.channel.detail.channelEntity
import org.apache.spark.listener.addListenerAction
import com.pharbers.common.clean.phSaveCleanAction
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.common.action.phResult2StringAction
import com.pharbers.spark.listener.sendProgress.sendXmppMultiProgress
import com.pharbers.spark.phSparkDriver

/**
  * @description:
  * @author: clock
  * @date: 2019-05-06 11:12
  */
case class phNhwaCleanJob(args: Map[String, String])(implicit send: channelEntity => Unit) extends sequenceJobWithMap {
    override val name: String = "phNhwaCleanJob"

    val job_id: String = args("job_id")
    val company_id: String = args("company_id")
    val user_id: String = args("user_id")
    val p_current: Double = args.getOrElse("p_current", "1").toDouble
    val p_total: Double = args.getOrElse("p_total", "1").toDouble

    val cpa_file: String = args("cpa_file")
    val hospital_file: String = args("hospital_file")
    val product_file: String = args("product_file")
    val pha_file: String = args("pha_file")
    val product_match_file: String = args("product_match_file")

    val cpa_erd_path: String = args("cpa_erd_path")
    val cpa_erd_name: String = args("cpa_erd_name")

    implicit val sp = phSparkDriver(job_id)
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

    val tranFun: SingleArgFuncArgs[pActionArgs, StringArgs] = phResult2StringAction.str2StrTranFun
    implicit val xp: Map[String, Any] => Unit = sendXmppMultiProgress(company_id, user_id, "clean", job_id)(p_current, p_total).multiProgress
    implicit val ss: MapArgs => Unit = { m =>
        val progress = m.getAs[StringArgs]("progress")
        println("progress = " + progress)
    }

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR") ::
                sendProgressAction(MapArgs(Map("progress" -> StringArgs("1")))) ::
                readCpa ::
                sendProgressAction(MapArgs(Map("progress" -> StringArgs("11")))) ::
                readHosp ::
                readProd ::
                readPha ::
                readProdMatch ::
                phNhwaCleanConcretAction(df) ::
                phSaveCleanAction(df) ::
                phResult2StringAction("phSaveCleanAction", tranFun) ::
                Nil
    }

}
