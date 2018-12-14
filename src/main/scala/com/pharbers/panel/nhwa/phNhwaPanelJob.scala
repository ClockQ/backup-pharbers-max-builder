package com.pharbers.panel.nhwa

import java.util.UUID

import akka.actor.Actor
import com.pharbers.channel.util.{sendEmTrait, sendTrait}
import com.pharbers.common.algorithm.max_path_obj
import com.pharbers.pactions.excel.input.{PhExcelXLSXCommonFormat, PhXlsxThirdSheetFormat}
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.generalactions._
import com.pharbers.pactions.generalactions.memory.phMemoryArgs
import com.pharbers.pactions.jobs._
import com.pharbers.panel.common.{phPanelInfo2Redis, phSavePanelJob}
import com.pharbers.panel.nhwa.format._
import org.apache.spark.listener
import org.apache.spark.listener.addListenerAction
import org.apache.spark.listener.progress.{sendMultiProgress, sendXmppMultiProgress}

/**
  * 1. read 2017年未出版医院名单.xlsx
  * 2. read universe_麻醉市场_online.xlsx
  * 3. read 匹配表
  * 4. read 补充医院
  * 5. read 通用名市场定义, 读第三页
  * 6. read CPA文件第一页
  * 7. read CPA文件第二页
  **/
case class phNhwaPanelJob(args: Map[String, String])(implicit _actor: Actor) extends sequenceJobWithMap {
    override val name: String = "phNhwaPanelJob"
    
    val temp_name: String = UUID.randomUUID().toString
    val temp_dir: String = max_path_obj.p_cachePath + temp_name + "/"
    val match_dir: String = max_path_obj.p_matchFilePath
    val source_dir: String = max_path_obj.p_clientPath
    
    val not_published_hosp_file: String = match_dir + args("not_published_hosp_file")
    val product_match_file: String = match_dir + args("product_match_file")
    val fill_hos_data_file: String = match_dir + args("fill_hos_data_file")
    val markets_match_file: String = match_dir + args("markets_match_file")
    val cpa_file: String = source_dir + args("cpa")
    val hosp_ID_file: String = match_dir + args("hosp_ID_file")
    val not_arrival_hosp_file: String = source_dir + args("not_arrival_hosp_file")
    
    lazy val ym: String = args("ym")
    lazy val mkt: String = args("mkt")
    lazy val user: String = args("user_id")
    lazy val job_id: String = args("job_id")
    lazy val company: String = args("company_id")
    lazy val p_total: Double = args("p_total").toDouble
    lazy val p_current: Double = args("p_current").toDouble
    
    implicit val companyArgs: phMemoryArgs = phMemoryArgs(company)
    implicit val xp: (sendEmTrait, Double, String) => Unit = sendXmppMultiProgress(company, user, "panel", job_id)(p_current, p_total).multiProgress

    /**
      * 1. read 未出版医院文件
      */
    val loadNotPublishedHosp: sequenceJob = new sequenceJob {
        override val name = "not_published_hosp_file"
        override val actions: List[pActionTrait] = readCsvAction(not_published_hosp_file, applicationName = job_id) :: Nil
    }
    
    /**
      * 2. read hosp_ID file
      */
    val load_hosp_ID_file: sequenceJob = new sequenceJob {
        override val name: String = "hosp_ID_file"
        override val actions: List[pActionTrait] = readCsvAction(hosp_ID_file, applicationName = job_id) :: Nil
    }
    
    /**
      * 3. read product match file
      */
    val loadProductMatchFile: sequenceJob = new sequenceJob {
        override val name = "product_match_file"
        val actions: List[pActionTrait] = readCsvAction(product_match_file, applicationName = job_id) :: Nil
    }
    
    /**
      * 4. read full hosp file
      */
    val loadFullHospFile: sequenceJob = new sequenceJob {
        override val name = "full_hosp_file"
        val actions: List[pActionTrait] = readCsvAction(fill_hos_data_file, applicationName = job_id) ::Nil
    }
    
    /**
      * 5. read market match file
      */
    val loadMarketMatchFile: sequenceJob = new sequenceJob {
        override val name = "markets_match_file"
        val actions: List[pActionTrait] = readCsvAction(markets_match_file, applicationName = job_id) :: Nil
    }
    
    /**
      * 6. read CPA文件第一页
      */
    val readCpa: sequenceJob = new sequenceJob {
        override val name = "cpa"
        override val actions: List[pActionTrait] = readCsvAction(cpa_file, applicationName = job_id) :: Nil
    }
    
    /**
      * 7. read CPA文件第二页
      */
    val readNotArrivalHosp: sequenceJob = new sequenceJob {
        override val name = "not_arrival_hosp_file"
        override val actions: List[pActionTrait] = readCsvAction(not_arrival_hosp_file, applicationName = job_id) :: Nil
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
    
    override val actions: List[pActionTrait] = {
                setLogLevelAction("ERROR", job_id) ::
                addListenerAction(listener.MaxSparkListener(0, 10, job_id), job_id) ::
                loadNotPublishedHosp ::
                addListenerAction(listener.MaxSparkListener(11, 20, job_id), job_id) ::
                load_hosp_ID_file ::
                addListenerAction(listener.MaxSparkListener(21, 30, job_id), job_id) ::
                loadProductMatchFile ::
                addListenerAction(listener.MaxSparkListener(31, 40, job_id), job_id) ::
                loadFullHospFile ::
                addListenerAction(listener.MaxSparkListener(41, 50, job_id), job_id) ::
                loadMarketMatchFile ::
                addListenerAction(listener.MaxSparkListener(51, 60, job_id), job_id) ::
                readCpa ::
                readNotArrivalHosp ::
                addListenerAction(listener.MaxSparkListener(61, 90, job_id), job_id) ::
                phNhwaPanelConcretJob(df) ::
                phSavePanelJob(df) ::
                addListenerAction(listener.MaxSparkListener(91, 99, job_id), job_id) ::
                phPanelInfo2Redis(df) ::
                Nil
    }
    
}