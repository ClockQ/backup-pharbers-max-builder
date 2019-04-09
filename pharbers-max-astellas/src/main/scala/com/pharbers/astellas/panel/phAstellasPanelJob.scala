package com.pharbers.astellas.panel

import com.pharbers.pactions.jobs._
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.generalactions._
import com.pharbers.channel.detail.channelEntity
import com.pharbers.common.action.phResult2StringJob
import com.pharbers.pactions.generalactions.memory.phMemoryArgs
import com.pharbers.common.panel.{phPanelInfo2Redis, phSavePanelJob}
import com.pharbers.spark.listener.sendProgress.sendXmppMultiProgress
import org.apache.spark.listener.addListenerAction

case class phAstellasPanelJob(args: Map[String, String])(implicit send: channelEntity => Unit) extends sequenceJobWithMap {
    override val name: String = "phAstellasPanelJob"

    val panel_path: String = args("panel_path")
    val panel_name: String = args("panel_name")

    val product_match_file: String = args("product_match_file")
    val markets_match_file: String = args("markets_match_file")
    val hosp_ID_file: String = args("hosp_ID_file")
    val hospital_file: String = args("hospital_file")
    val cpa_file: String = args("cpa_file")
    val gyc_file: String = args("gycx_file")

    val ym: String = args("ym")
    val mkt: String = args("mkt")
    val mkt_en: String = getMktEn(mkt)
    val user_id: String = args("user_id")
    val job_id: String = args("job_id")
    val company_id: String = args("company_id")
    val prod_name_lst: List[String] = args("prod_lst").split("#").toList
    val p_current: Double = args("p_current").toDouble
    val p_total: Double = args("p_total").toDouble

    def getMktEn(mkt: String): String = {
        mkt match {
            case "阿洛刻市场" => "Allelock"
            case "米开民市场" => "Mycamine"
            case "普乐可复市场" => "Prograf"
            case "佩尔市场" => "Perdipine"
            case "哈乐市场" => "Harnal"
            case "痛风市场" => "Gout"
            case "卫喜康市场" => "Vesicare"
            case "Grafalon市场" => "Grafalon"
            case "前列腺癌市场" => "前列腺癌"
        }
    }

    //1. read 产品匹配表
    val load_product_match_file: sequenceJob = new sequenceJob {
        override val name = "product_match_file"
        override val actions: List[pActionTrait] = readCsvAction(product_match_file, applicationName = job_id) :: Nil
    }

    //2. read 市场匹配表
    val load_markets_match_file: sequenceJob = new sequenceJob {
        override val name = "markets_match_file"
        override val actions: List[pActionTrait] = readCsvAction(markets_match_file, applicationName = job_id) :: Nil
    }

    //3. read If_panel_all文件
    val load_hosp_ID: sequenceJob = new sequenceJob {
        override val name: String = "hosp_ID_file"
        override val actions: List[pActionTrait] = readCsvAction(hosp_ID_file, applicationName = job_id) :: Nil
    }

    //4. read hospital_file文件 三源匹配表
    val load_hospital_file: sequenceJob = new sequenceJob {
        override val name = "hospital_file"
        override val actions: List[pActionTrait] = readCsvAction(hospital_file, applicationName = job_id) :: Nil
    }

    //5. read CPA源文件
    val load_cpa: sequenceJob = new sequenceJob {
        override val name = "cpa"
        override val actions: List[pActionTrait] = readCsvAction(cpa_file, applicationName = job_id) :: Nil
    }

    //6. read GYC源文件
    val load_gycx: sequenceJob = new sequenceJob {
        override val name = "gycx"
        override val actions: List[pActionTrait] = readCsvAction(gyc_file, applicationName = job_id) :: Nil
    }

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
        ) ++ Map(
            "mkt_en" -> StringArgs(mkt_en)
        )
    )

    val tranFun: SingleArgFuncArgs[pActionArgs, StringArgs] = phResult2StringJob.str2StrTranFun
    implicit val companyArgs: phMemoryArgs = phMemoryArgs(company_id)
    implicit val xp: Map[String, Any] => Unit = sendXmppMultiProgress(company_id, user_id, "panel", job_id)(p_current, p_total).multiProgress

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                addListenerAction(1, 10, job_id) ::
                load_product_match_file ::
                addListenerAction(11, 20, job_id) ::
                load_markets_match_file ::
                addListenerAction(21, 30, job_id) ::
                load_hosp_ID ::
                addListenerAction(31, 40, job_id) ::
                load_hospital_file ::
                addListenerAction(41, 50, job_id) ::
                load_cpa ::
                addListenerAction(51, 60, job_id) ::
                load_gycx ::
                addListenerAction(61, 70, job_id) ::
                phAstellasPanelConcretJob(df) ::
                addListenerAction(71, 80, job_id) ::
                phSavePanelJob(df) ::
                addListenerAction(81, 90, job_id) ::
                phPanelInfo2Redis(df) ::
                addListenerAction(91, 99, job_id) ::
                phResult2StringJob("phPanelInfo2Redis", tranFun) ::
                Nil
    }
}