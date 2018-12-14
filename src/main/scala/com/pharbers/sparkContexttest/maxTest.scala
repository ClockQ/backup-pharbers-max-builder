package com.pharbers.sparkContexttest

import java.util.UUID

import akka.actor.Actor
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionArgs, pActionTrait}
import com.pharbers.panel.pfizer.phPfizerPanelJob
import com.pharbers.builder.phMarketTable.Builderimpl

case class maxTest(implicit _actor: Actor) {
    val company: String = "5b028f95ed925c2c705b85ba"
    val mkt: String = "INF"
    val builderimpl = Builderimpl(company)
    import builderimpl._
    
    val map: Map[String, String] = Map(
        "company" -> company,
        "mkt" -> mkt,
        "user" -> "user",
        "job_id" -> UUID.randomUUID().toString,
        "cpa" -> "pfizer/pha_config_repository1804/Pfizer_201804_CPA.xlsx",
        "gycx" -> "pfizer/pha_config_repository1804/Pfizer_201804_Gycx.xlsx",
        "universe_file" -> "pfizer/pha_config_repository1804/Pfizer_Universe_INF.xlsx",
        "product_match_file" -> "pfizer/pha_config_repository1804/Pfizer_ProductMatchTable.xlsx",
        "fill_hos_data_file" -> "pfizer/pha_config_repository1804/Pfizer_2018_FullHosp.txt",
        "markets_match_file" -> "pfizer/pha_config_repository1804/Pfizer_MarketMatchTable.xlsx",
        "not_published_hosp_file" -> "pfizer/pha_config_repository1804/Pfizer_201804_CPA.xlsx",
        "hosp_ID" -> "pfizer/pha_config_repository1804/Pfizer_2018_If_panel_all_INF.xlsx",
        "ym" -> "201804"
    )

    val data: List[Map[String, String]] = List(map)
    def doPanel(mapping: Map[String, String]): String = {
        val panelInstMap = getPanelInst(mkt)
        val ckArgLst = panelInstMap("source").split("#").toList ::: panelInstMap("args").split("#").toList ::: Nil
        val args = mapping ++ panelInstMap ++ data.find(x => company == x("company") && mkt == x("market")).get
        
        if(!parametCheck(ckArgLst, args)(m => ck_base(m) && ck_panel(m)))
            throw new Exception("input wrong")
        
        val clazz: String = panelInstMap("instance")
        val result = impl(clazz, args).perform(MapArgs(Map().empty))
                .asInstanceOf[MapArgs]
                .get("phSavePanelJob")
                .asInstanceOf[StringArgs].get
        //        phSparkDriver().sc.stop()
        result
    }
    val panel = doPanel(map)
}
