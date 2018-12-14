package com.pharbers.nhwa.panel

import akka.actor.ActorSelection
import com.pharbers.pactions.jobs._
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.generalactions._
import org.apache.spark.listener.helper.ListenerHelper
import com.pharbers.pactions.generalactions.memory.phMemoryArgs
import org.apache.spark.listener.{MaxSparkListener, addListenerAction}
import org.apache.spark.listener.sendProgress.{sendXmppMultiProgress, sendXmppSingleProgress}

/**
  * 1. read 2017年未出版医院名单.xlsx
  * 2. read universe_麻醉市场_online.xlsx
  * 3. read 匹配表
  * 4. read 补充医院
  * 5. read 通用名市场定义, 读第三页
  * 6. read CPA文件第一页
  * 7. read CPA文件第二页
  **/
case class phNhwaPanelJob(args: Map[String, String])(implicit sendActor: ActorSelection) extends sequenceJobWithMap {
    override val name: String = "phNhwaPanelJob"

    val panel_path: String = args("panel_path")
    val panel_name: String = args("panel_name")
    val not_published_hosp_file: String = args("not_published_hosp_file")
    val product_match_file: String = args("product_match_file")
    val fill_hos_data_file: String = args("fill_hos_data_file")
    val markets_match_file: String = args("markets_match_file")
    val cpa_file: String = args("cpa_file")
    val hosp_ID_file: String = args("hosp_ID_file")
    val not_arrival_hosp_file: String = args("not_arrival_hosp_file")

    lazy val ym: String = args("ym")
    lazy val mkt: String = args("mkt")
    lazy val user_id: String = args("user_id")
    lazy val job_id: String = args("job_id")
    lazy val company_id: String = args("company_id")
    lazy val p_current: Double = args("p_current").toDouble
    lazy val p_total: Double = args("p_total").toDouble

    implicit val companyArgs: phMemoryArgs = phMemoryArgs(company_id)

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
        val actions: List[pActionTrait] = readCsvAction(fill_hos_data_file, applicationName = job_id) :: Nil
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
            "user" -> StringArgs(user_id),
            "name" -> StringArgs(panel_name),
            "company" -> StringArgs(company_id),
            "job_id" -> StringArgs(job_id),
            "panel_path" -> StringArgs(panel_path)
        )
    )

    implicit val xp: Map[String, Any] => Unit = sendXmppMultiProgress(company_id, user_id, "ymCalc", job_id)(p_current, p_total).multiProgress

    override val actions: List[pActionTrait] = {
        setLogLevelAction("ERROR", job_id) ::
                addListenerAction(MaxSparkListener(ListenerHelper(0, 10), job_id)) ::
                loadNotPublishedHosp ::
                addListenerAction(MaxSparkListener(ListenerHelper(11, 20), job_id)) ::
                load_hosp_ID_file ::
                addListenerAction(MaxSparkListener(ListenerHelper(21, 30), job_id)) ::
                loadProductMatchFile ::
                addListenerAction(MaxSparkListener(ListenerHelper(31, 40), job_id)) ::
                loadFullHospFile ::
                addListenerAction(MaxSparkListener(ListenerHelper(41, 50), job_id)) ::
                loadMarketMatchFile ::
                addListenerAction(MaxSparkListener(ListenerHelper(51, 60), job_id)) ::
                readCpa ::
                readNotArrivalHosp ::
                addListenerAction(MaxSparkListener(ListenerHelper(61, 90), job_id)) ::
                phNhwaPanelConcretJob(df) ::
                phSavePanelJob(df) ::
                addListenerAction(MaxSparkListener(ListenerHelper(91, 99), job_id)) ::
                phPanelInfo2Redis(df) ::
                Nil
    }

}