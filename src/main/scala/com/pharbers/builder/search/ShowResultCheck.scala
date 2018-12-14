package com.pharbers.builder.search

import com.pharbers.search.{phMaxResultInfo, phMaxSearchTrait}

/**
  * @ ProjectName pharbers-max.com.pharbers.builder.search.ShowResultCheck
  * @ author jeorch
  * @ date 18-9-26
  * @ Description: TODO
  */
class ShowResultCheck extends phMaxSearchTrait {

    def searchResultCheck(company_id: String, ym: String, market: String): (Map[String, Any], List[Map[String, Any]], List[Map[String, Any]], Map[String, Any]) = {
        val max = phMaxResultInfo(company_id, ym, market)

        if (max.getMaxResultSales == 0.0){
            (Map.empty, List.empty, List.empty, Map.empty)
        } else {
            val indicators = Map(
                "marketSumSales" ->
                    Map(
                        "currentNumber" -> getFormatSales(max.getMaxResultSales),
                        "lastYearPercentage" -> max.getLastYearResultSalesPercentage
                    )
                ,
                "productSales" ->
                    Map(
                        "currentNumber" -> getFormatSales(max.getCurrCompanySales),
                        "lastYearPercentage" -> max.getLastYearCurrCompanySalesPercentage
                    )

            )
            val trend = max.getLastSeveralMonthResultSalesLst(12)
            val region = max.getProvLstMap.map(item =>
                Map(
                    "name" -> item("Province"),
                    "value" -> item("TotalSales"),
                    "productSales" -> item("CompanySales"),
                    "percentage" -> item("Share")
                )
            )
            val mirror = Map(
                "provinces" ->
                    Map(
                        "current" ->
                            max.getProvLstMap.take(10).map(item => Map(
                                "area" -> item("Province"),
                                "marketSales" -> item("TotalSales"),
                                "productSales" -> item("CompanySales"),
                                "percentage" -> item("Share")
                            )),
                        "lastyear" ->
                            max.getProvLstMap.take(10).map(item => Map(
                                "area" -> item("Province"),
                                "marketSales" -> item("lastYearYMTotalSales"),
                                "productSales" -> item("CompanySales"),
                                "percentage" -> item("lastYearYMShare")
                            ))
                    ),
                "city" ->
                    Map(
                        "current" ->
                            max.getCityLstMap.take(10).map(item => Map(
                                "area" -> item("City"),
                                "marketSales" -> item("TotalSales"),
                                "productSales" -> item("CompanySales"),
                                "percentage" -> item("Share")
                            )),
                        "lastyear" ->
                            max.getCityLstMap.take(10).map(item => Map(
                                "area" -> item("City"),
                                "marketSales" -> item("lastYearYMTotalSales"),
                                "productSales" -> item("CompanySales"),
                                "percentage" -> item("lastYearYMShare")
                            ))
                    )
            )

            (indicators, trend, region, mirror)
        }
    }

}
