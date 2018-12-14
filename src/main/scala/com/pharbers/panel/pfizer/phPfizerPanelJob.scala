package com.pharbers.panel.pfizer

import java.util.UUID

import akka.actor.Actor
import com.pharbers.channel.util.sendEmTrait
import com.pharbers.panel.pfizer.actions._
import com.pharbers.pactions.generalactions._
import com.pharbers.common.algorithm.max_path_obj
import org.apache.spark.listener.progress.{sendMultiProgress, sendXmppMultiProgress, sendXmppSingleProgress}
import com.pharbers.pactions.generalactions.memory.phMemoryArgs
import com.pharbers.panel.common.{phPanelInfo2Redis, phSavePanelJob}
import org.apache.spark.listener.{MaxSparkListener, addListenerAction}
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionTrait}
import com.pharbers.pactions.jobs.{sequenceJob, sequenceJobWithMap}
import com.pharbers.panel.pfizer.format.{phPfizerCpaFormat, phPfizerGycxFormat}
import com.pharbers.pactions.excel.input.{PhExcelXLSXCommonFormat, PhXlsxSecondSheetFormat}

/**
  * Created by jeorch on 18-4-18.
  * Modify by clock on 18-5-21
  */
case class phPfizerPanelJob(args: Map[String, String])(implicit _actor: Actor) extends sequenceJobWithMap {
    override val name: String = "phPfizerPanelJob"
    
    val temp_name: String = UUID.randomUUID().toString
    val temp_dir: String = max_path_obj.p_cachePath + temp_name + "/"
    val match_dir: String = max_path_obj.p_matchFilePath
    val source_dir: String = max_path_obj.p_clientPath
    
    val universe_file: String = match_dir + args("universe_file")
    val product_match_file: String = match_dir + args("product_match_file")
    val markets_match_file: String = match_dir + args("markets_match_file")
    val hosp_ID_file: String = match_dir + args("hosp_ID")
    val not_arrival_hosp_file: String = source_dir + args("not_arrival_hosp_file")
    /**
      * 不同年份有不同的补充文件,是否需要进行历史补充医院的合并?
      * ToDo:为了满足用户不仅仅可以计算当月,还可以计算历史月份
      */
    val fill_hos_data_file: String = match_dir + args("fill_hos_data_file")
    val pfc_match_file: String = match_dir + args("pfc_match_file")
    val cpa_file: String = source_dir + args("cpa")
    val gyc_file: String = source_dir + args("gycx")
    
    lazy val ym: String = args("ym")
    lazy val mkt: String = args("mkt")
    lazy val user: String = args("user_id")
    lazy val job_id: String = args("job_id")
    lazy val company: String = args("company_id")
    lazy val p_total: Double = args("p_total").toDouble
    lazy val p_current: Double = args("p_current").toDouble
    
    implicit val companyArgs: phMemoryArgs = phMemoryArgs(company)
    implicit val xp: (sendEmTrait, Double, String) => Unit = sendXmppMultiProgress(company, user, "panel", job_id)(p_current, p_total).multiProgress
    
    
    //1. read hosp_ID file
    val load_hosp_ID_file: sequenceJob = new sequenceJob {
        override val name: String = "hosp_ID_file"
        override val actions: List[pActionTrait] =
            readCsvAction(hosp_ID_file, applicationName = job_id) :: Nil
    }
    
    //2. read product_match_file
    val load_product_match_file: sequenceJob = new sequenceJob {
        override val name = "product_match_file"
        override val actions: List[pActionTrait] =
                        readCsvAction(product_match_file, applicationName = job_id) :: Nil
    }
    
    //3. read markets_match_file
    val load_markets_match_file: sequenceJob = new sequenceJob {
        override val name = "markets_match_file"
        override val actions: List[pActionTrait] =
                        readCsvAction(markets_match_file, applicationName = job_id) :: Nil
    }
    
    //4. read full_hosp_file
    val load_full_hosp_file: sequenceJob = new sequenceJob {
        override val name = "full_hosp_file"
        override val actions: List[pActionTrait] =
                        readCsvAction(fill_hos_data_file, 31.toChar.toString, applicationName = job_id) :: Nil
    }
    
    //5. read pfc_match_file
    val load_pfc_match_file: sequenceJob = new sequenceJob {
        override val name = "pfc_match_file"
        override val actions: List[pActionTrait] =
                        readCsvAction(pfc_match_file, applicationName = job_id) :: Nil
    }
    
    //6. read CPA文件第一页
    val readCpa: sequenceJob = new sequenceJob {
        override val name = "cpa"
        override val actions: List[pActionTrait] =
            readCsvAction(cpa_file, applicationName = job_id) :: Nil
    }
    
    //7. read CPA文件第二页
    val readNotArrivalHosp: sequenceJob = new sequenceJob {
        override val name = "not_arrival_hosp_file"
        override val actions: List[pActionTrait] =
            readCsvAction(not_arrival_hosp_file, applicationName = job_id) :: Nil
    }
    
    //8. read GYCX文件
    val readGyc: sequenceJob = new sequenceJob {
        override val name = "gyc"
        override val actions: List[pActionTrait] =
            readCsvAction(gyc_file, applicationName = job_id) :: Nil
    }
    
    val df = MapArgs(
        Map(
            "ym" -> StringArgs(ym),
            "mkt" -> StringArgs(mkt),
            "user" -> StringArgs(user),
            "name" -> StringArgs(temp_name),
            "company" -> StringArgs(company),
            "job_id" -> StringArgs(job_id)
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
    
    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                addListenerAction(MaxSparkListener(0, 10, job_id), job_id) ::
                load_hosp_ID_file ::
                addListenerAction(MaxSparkListener(11, 20, job_id), job_id) ::
                load_product_match_file ::
                addListenerAction(MaxSparkListener(21, 30, job_id), job_id) ::
                load_markets_match_file ::
                addListenerAction(MaxSparkListener(31, 40, job_id), job_id) ::
                load_full_hosp_file ::
                addListenerAction(MaxSparkListener(41, 50, job_id), job_id) ::
                load_pfc_match_file ::
                addListenerAction(MaxSparkListener(51, 60, job_id), job_id) ::
                readCpa ::
                addListenerAction(MaxSparkListener(61, 70, job_id), job_id) ::
                readNotArrivalHosp ::
                addListenerAction(MaxSparkListener(71, 80, job_id), job_id) ::
                readGyc ::
                splitMktJobsMap.getOrElse(mkt, throw new Exception(s"undefined market=$mkt")) ::
                addListenerAction(MaxSparkListener(81, 90, job_id), job_id) ::
                phPfizerPanelCommonAction(df) ::
                phSavePanelJob(df) ::
                addListenerAction(MaxSparkListener(91, 99, job_id), job_id) ::
                phPanelInfo2Redis(df) ::
                Nil
    }
}
