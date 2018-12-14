package com.pharbers.builder.search

import com.pharbers.search.phPanelResultInfo

/**
  * @ ProjectName pharbers-max.com.pharbers.builder.search.ShowSampleCheck
  * @ author jeorch
  * @ date 18-9-26
  * @ Description: TODO
  */
class ShowSampleCheck {
    def searchSimpleCheck(user_id: String, company_id: String, ym: String, market: String): (Map[String, Any], Map[String, Any], Map[String, Any], List[Map[String, Any]]) = {
        val month = ym.takeRight(2).toInt

        val panelInfo = phPanelResultInfo(user_id, company_id, ym, market)
        import panelInfo._

        val hospitalMap = Map(
            "baselines" -> baseLine("HOSP_ID"),
            "samplenumbers" -> setValue2Array(month - 1, getHospCount.toString),
            "currentNumber" -> getHospCount,
            "lastYearNumber" -> getLastYearHospCount(month)
        )
        val productMap = Map(
            "baselines" -> baseLine("Prod_Name"),
            "samplenumbers" -> setValue2Array(month - 1, getProdCount.toString),
            "currentNumber" -> getProdCount,
            "lastYearNumber" -> getLastYearProdCount(month)
        )
        val salesMap = Map(
            "baselines" -> baseLine("Sales"),
            "samplenumbers" -> setValue2Array(month - 1, getFormatSales(getPanelSales).toString),
            "currentNumber" -> getFormatSales(getPanelSales),
            "lastYearNumber" -> getLastYearPanelSales(month)
        )
        val notFindHospital = panelInfo.getNotPanelHospLst.zipWithIndex.map(x => {
            val temp = x._1.replace("[", "").replace("]", "").split(",")
            Map(
                "index" -> (x._2 + 1),
                "hospitalName" -> temp(0)
            )
        })

        (hospitalMap, productMap, salesMap, notFindHospital)
    }
}
