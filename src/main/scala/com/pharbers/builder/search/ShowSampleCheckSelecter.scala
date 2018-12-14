package com.pharbers.builder.search

import com.pharbers.builder.phMarketTable.{phMarketDBTrait, phMarketManager}
import com.pharbers.driver.PhRedisDriver

/**
  * @ ProjectName pharbers-max.com.pharbers.builder.search.ShowSampleCheckSelecter
  * @ author jeorch
  * @ date 18-9-26
  * @ Description: TODO
  */
class ShowSampleCheckSelecter extends phMarketDBTrait with phMarketManager  {
    def getYmsAndMkts(job_id: String, company_id: String): (List[String], List[String]) = {
        val ymLst = new PhRedisDriver().getSetAllValue(job_id + "ym").toList.sorted
        (ymLst, getAllMarkets(company_id))
    }
}
