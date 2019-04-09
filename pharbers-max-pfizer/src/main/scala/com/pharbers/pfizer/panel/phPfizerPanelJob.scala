package com.pharbers.pfizer.panel

import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.generalactions._
import com.pharbers.channel.detail.channelEntity
import com.pharbers.common.action.phResult2StringJob
import com.pharbers.pactions.generalactions.memory.phMemoryArgs
import com.pharbers.common.panel.{phPanelInfo2Redis, phSavePanelJob}
import com.pharbers.pactions.jobs.{sequenceJob, sequenceJobWithMap}
import com.pharbers.pfizer.panel.actions.{phPfizerPanelCommonAction, phPfizerPanelNoSplitAction, phPfizerPanelSplitChildMarketAction, phPfizerPanelSplitFatherMarketAction}
import com.pharbers.spark.listener.sendProgress.sendXmppMultiProgress
import org.apache.spark.listener.addListenerAction

case class phPfizerPanelJob(args: Map[String, String])
                           (implicit send: channelEntity => Unit) extends sequenceJobWithMap {

    override val name: String = "phPfizerPanelJob"

    val panel_path: String = args("panel_path")
    val panel_name: String = args("panel_name")

    val hosp_ID_file: String = args("hosp_ID_file")
    val product_match_file: String = args("product_match_file")
    val markets_match_file: String = args("markets_match_file")
    val fill_hos_data_file: String = args("fill_hos_data_file")
    val pfc_match_file: String = args("pfc_match_file")
    val cpa_file: String = args("cpa_file")
    val not_arrival_hosp_file: String = args("not_arrival_hosp_file")
    val gycx_file: String = args("gycx_file")

    val ym: String = args("ym")
    val mkt: String = args("mkt")
    val user_id: String = args("user_id")
    val job_id: String = args("job_id")
    val company_id: String = args("company_id")
    val prod_name_lst: List[String] = args("prod_lst").split("#").toList
    val p_current: Double = args("p_current").toDouble
    val p_total: Double = args("p_total").toDouble

    val full_hosp_delimiter: String = args.getOrElse("full_hosp_delimiter", 31.toChar.toString)

    val df = MapArgs(
        Map(
            "ym" -> StringArgs(ym),
            "mkt" -> StringArgs(mkt),
            "user_id" -> StringArgs(user_id),
            "job_id" -> StringArgs(job_id),
            "company_id" -> StringArgs(company_id),
            "panel_name" -> StringArgs(panel_name),
            "panel_path" -> StringArgs(panel_path),
            "prod_name" -> ListArgs(prod_name_lst.map(StringArgs))
        )
    )

    val splitMktJobsMap: Map[String, pActionTrait] = Map(
        "AI_R_zith" -> phPfizerPanelNoSplitAction(df),
        "AI_S" -> phPfizerPanelNoSplitAction(df),
        "CNS_Z" -> phPfizerPanelNoSplitAction(df),
        "ELIQUIS" -> phPfizerPanelNoSplitAction(df),
        "INF" -> phPfizerPanelNoSplitAction(df),
        "LD" -> phPfizerPanelNoSplitAction(df),
        "ONC_other" -> phPfizerPanelNoSplitAction(df),
        "ONC_aml" -> phPfizerPanelNoSplitAction(df),
        "PAIN_lyrica" -> phPfizerPanelNoSplitAction(df),
        "Specialty_champix" -> phPfizerPanelNoSplitAction(df),
        "Specialty_other" -> phPfizerPanelNoSplitAction(df),
        "Urology_other" -> phPfizerPanelNoSplitAction(df),
        "Urology_viagra" -> phPfizerPanelNoSplitAction(df),

        "PAIN_C" -> phPfizerPanelSplitChildMarketAction(df),
        "HTN2" -> phPfizerPanelSplitChildMarketAction(df),
        "AI_D" -> phPfizerPanelSplitChildMarketAction(df),
        "ZYVOX" -> phPfizerPanelSplitChildMarketAction(df),

        "PAIN_other" -> phPfizerPanelSplitFatherMarketAction(df),
        "HTN" -> phPfizerPanelSplitFatherMarketAction(df),
        "AI_W" -> phPfizerPanelSplitFatherMarketAction(df),
        "AI_R_other" -> phPfizerPanelSplitFatherMarketAction(df),

        "CNS_R" -> phPfizerPanelNoSplitAction(df),
        "DVP" -> phPfizerPanelNoSplitAction(df)
    )

    //1. read hosp_ID file
    val load_hosp_ID_file: sequenceJob = new sequenceJob {
        override val name: String = "hosp_ID_file"
        override val actions: List[pActionTrait] = readCsvAction(hosp_ID_file, applicationName = job_id) :: Nil
    }

    //2. read product_match_file
    val load_product_match_file: sequenceJob = new sequenceJob {
        override val name = "product_match_file"
        override val actions: List[pActionTrait] = readCsvAction(product_match_file, applicationName = job_id) :: Nil
    }

    //3. read markets_match_file
    val load_markets_match_file: sequenceJob = new sequenceJob {
        override val name = "markets_match_file"
        override val actions: List[pActionTrait] = readCsvAction(markets_match_file, applicationName = job_id) :: Nil
    }

    //4. read full_hosp_file
    val load_full_hosp_file: sequenceJob = new sequenceJob {
        override val name = "full_hosp_file"
        override val actions: List[pActionTrait] = readCsvAction(fill_hos_data_file, delimiter = full_hosp_delimiter, applicationName = job_id) :: Nil
    }

    //5. read pfc_match_file
    val load_pfc_match_file: sequenceJob = new sequenceJob {
        override val name = "pfc_match_file"
        override val actions: List[pActionTrait] = readCsvAction(pfc_match_file, applicationName = job_id) :: Nil
    }

    //6. read cpa
    val readCpa: sequenceJob = new sequenceJob {
        override val name = "cpa"
        override val actions: List[pActionTrait] = readCsvAction(cpa_file, applicationName = job_id) :: Nil
    }

    //7. read not_arrival_hosp_file
    val readNotArrivalHosp: sequenceJob = new sequenceJob {
        override val name = "not_arrival_hosp_file"
        override val actions: List[pActionTrait] = readCsvAction(not_arrival_hosp_file, applicationName = job_id) :: Nil
    }

    //8. read GYCX文件
    val readGyc: sequenceJob = new sequenceJob {
        override val name = "gycx"
        override val actions: List[pActionTrait] = readCsvAction(gycx_file, applicationName = job_id) :: Nil
    }

    val tranFun: SingleArgFuncArgs[pActionArgs, StringArgs] = phResult2StringJob.str2StrTranFun
    implicit val companyArgs: phMemoryArgs = phMemoryArgs(company_id)
    implicit val xp: Map[String, Any] => Unit = sendXmppMultiProgress(company_id, user_id, "panel", job_id)(p_current, p_total).multiProgress

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                addListenerAction(1, 5, job_id) ::
                load_hosp_ID_file ::
                addListenerAction(6, 10, job_id) ::
                load_product_match_file ::
                addListenerAction(11, 15, job_id) ::
                load_markets_match_file ::
                addListenerAction(16, 20, job_id) ::
                load_full_hosp_file ::
                addListenerAction(21, 25, job_id) ::
                load_pfc_match_file ::
                addListenerAction(26, 35, job_id) ::
                readCpa ::
                addListenerAction(36, 45, job_id) ::
                readNotArrivalHosp ::
                addListenerAction(46, 55, job_id) ::
                readGyc ::
                addListenerAction(56, 60, job_id) ::
                splitMktJobsMap.getOrElse(mkt, throw new Exception(s"undefined market=$mkt")) ::
                addListenerAction(61, 70, job_id) ::
                phPfizerPanelCommonAction(df) ::
                addListenerAction(71, 80, job_id) ::
                phSavePanelJob(df) ::
                addListenerAction(81, 90, job_id) ::
                phPanelInfo2Redis(df) ::
                addListenerAction(91, 99, job_id) ::
                phResult2StringJob("phPanelInfo2Redis", tranFun) ::
                Nil
    }
}
