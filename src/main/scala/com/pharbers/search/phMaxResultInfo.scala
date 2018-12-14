package com.pharbers.search

import java.util.Base64

import com.pharbers.driver.PhRedisDriver
import com.pharbers.sercuity.Sercurity

/**
  * Created by jeorch on 18-5-14.
  */
case class phMaxResultInfo(company: String, ym:String, mkt: String) extends phMaxSearchTrait {

    val rd = new PhRedisDriver()
    val singleJobKey: String = Base64.getEncoder.encodeToString((company +"#"+ ym +"#"+ mkt).getBytes())
    val max_sales_city_lst_key: String = Sercurity.md5Hash(company + ym + mkt + "max_sales_city_lst_key")
    val max_sales_prov_lst_key: String = Sercurity.md5Hash(company + ym + mkt + "max_sales_prov_lst_key")
    val company_sales_city_lst_key: String = Sercurity.md5Hash(company + ym + mkt + "company_sales_city_lst_key")
    val company_sales_prov_lst_key: String = Sercurity.md5Hash(company + ym + mkt + "company_sales_prov_lst_key")

    val lastYearYM: String = getLastYearYM(ym)
    val lastYearSingleJobKey: String = Base64.getEncoder.encodeToString((company +"#"+ lastYearYM +"#"+ mkt).getBytes())

    def getMaxResultSales: Double = rd.getMapValue(singleJobKey, "max_sales") match {
        case null => 0.0
        case x => x.toDouble
    }
    def getCurrCompanySales: Double = rd.getMapValue(singleJobKey, "max_company_sales") match {
        case null => 0.0
        case x => x.toDouble
    }

    val getLastYearResultSales : Double = getHistorySalesByRange("NATION_SALES", lastYearSingleJobKey)
    val getLastYearCurrCompanySales : Double = getHistorySalesByRange("NATION_COMPANY_SALES", lastYearSingleJobKey)

    def getLastYearResultSalesPercentage: Double = getLastYearResultSales match {
            case 0.0 => 0.0
            case _ => (getMaxResultSales - getLastYearResultSales)/getLastYearResultSales
        }

    def getLastYearCurrCompanySalesPercentage: Double = getLastYearCurrCompanySales match {
            case 0.0 => 0.0
            case _ => (getCurrCompanySales - getLastYearCurrCompanySales)/getLastYearCurrCompanySales
        }

    def getLastSeveralMonthResultSalesLst(severalCount: Int): List[Map[String, Any]] = {
        val tmpLst = getLastSeveralMonthYM(severalCount, ym).map(singleYM => {
            val tempSingleJobKey = Base64.getEncoder.encodeToString((company + "#" + singleYM + "#" + mkt).getBytes())
            val tempMaxSales = getHistorySalesByRange("NATION_SALES", tempSingleJobKey)
            val tempCompanySales = getHistorySalesByRange("NATION_COMPANY_SALES", tempSingleJobKey)
            val tempPercentage = tempMaxSales match {
                case 0.0 => 0.0
                case _ => tempCompanySales / tempMaxSales
            }
            Map("date" -> singleYM, "percentage" -> getFormatShare(tempPercentage), "marketSales" -> getFormatSales(tempMaxSales))
        })
        Map("date" -> ym, "percentage" -> getFormatShare(getCurrCompanySales/getMaxResultSales), "marketSales" -> getFormatSales(getMaxResultSales))::tmpLst
    }.reverse

    def getCityLstMap: List[Map[String, Any]] = {
        val currCompanyCityLst = rd.getListAllValue(company_sales_city_lst_key).map({x =>
            val temp = x.replace("[","").replace("]","").split(",")
            Map("City" -> temp(0), "Sales" -> temp(1))
        })
        val lastYearCompanyCityLst = getAreaSalesByRange("CITY_COMPANY_SALES", lastYearSingleJobKey) match {
            case Nil => Nil
            case lst => lst.map({x =>
                Map("City" -> x("Area"), "Sales" -> x("Sales"))
            })
        }
        val lastYearMaxCityLst = getAreaSalesByRange("CITY_SALES", lastYearSingleJobKey) match {
            case Nil => Nil
            case lst => lst.map({x =>
                Map("City" -> x("Area"), "Sales" -> x("Sales"))
            })
        }
        rd.getListAllValue(max_sales_city_lst_key).map({x =>
            val temp = x.replace("[","").replace("]","").split(",")
            val companyCityMap: Map[String, String] = currCompanyCityLst.find(x => x("City") == temp(0)).getOrElse(Map("Sales" -> "0"))
            val tempShare = companyCityMap("Sales").toString.toDouble/temp(1).toDouble
            val tempLastYearMaxSalesMap = lastYearMaxCityLst.find(x => x("City") == temp(0)).getOrElse(Map("Sales" -> "0"))
            val tempLastYearCompanySalesMap = lastYearCompanyCityLst.find(x => x("City") == temp(0)).getOrElse(Map("Sales" -> "0"))
            val tempLastYearShare: Double = if (tempLastYearMaxSalesMap("Sales").toDouble == 0.0) 0.0 else tempLastYearCompanySalesMap("Sales").toDouble / tempLastYearMaxSalesMap("Sales").toDouble
            Map(
                "City" -> temp(0),
                "CompanySales" -> getFormatSales(companyCityMap("Sales").toDouble),
                "TotalSales" -> getFormatSales(temp(1).toDouble),
                "Share" -> getFormatShare(tempShare),
                "lastYearYMCompanySales" -> getFormatSales(tempLastYearCompanySalesMap("Sales").toDouble),
                "lastYearYMTotalSales" -> getFormatSales(tempLastYearMaxSalesMap("Sales").toDouble),
                "lastYearYMShare" -> getFormatShare(tempLastYearShare)
            )
        })
    }

    def getProvLstMap: List[Map[String, Any]] = {
        val currCompanyProvLst = rd.getListAllValue(company_sales_prov_lst_key).map({x =>
            val temp = x.replace("[","").replace("]","").split(",")
            Map("Province" -> temp(0), "Sales" -> temp(1))
        })
        val lastYearCompanyProvLst = getAreaSalesByRange("PROVINCE_COMPANY_SALES", lastYearSingleJobKey) match {
            case Nil => Nil
            case lst => lst.map({x =>
                Map("Province" -> x("Area"), "Sales" -> x("Sales"))
            })
        }
        val lastYearMaxProvLst = getAreaSalesByRange("PROVINCE_SALES", lastYearSingleJobKey) match {
            case Nil => Nil
            case lst => lst.map({x =>
                Map("Province" -> x("Area"), "Sales" -> x("Sales"))
            })
        }

        rd.getListAllValue(max_sales_prov_lst_key).map{ x =>
            val temp = x.replace("[","").replace("]","").split(",")
            val companyProvMap = currCompanyProvLst.find(x => x("Province") == temp(0)).getOrElse(Map("Sales" -> "0"))
            val tempShare: Double = companyProvMap("Sales").toDouble/temp(1).toDouble
            val tempLastYearMaxSalesMap = lastYearMaxProvLst.find(x => x("Province") == temp(0)).getOrElse(Map("Sales" -> "0"))
            val tempLastYearCompanySalesMap = lastYearCompanyProvLst.find(x => x("Province") == temp(0)).getOrElse(Map("Sales" -> "0"))
            val tempLastYearShare = if (tempLastYearMaxSalesMap("Sales").toDouble == 0.0) 0.0
                else tempLastYearCompanySalesMap("Sales").toDouble / tempLastYearMaxSalesMap("Sales").toDouble

            Map(
                "Province" -> temp(0),
                "CompanySales" -> getFormatSales(companyProvMap("Sales").toDouble),
                "TotalSales" -> getFormatSales(temp(1).toDouble),
                "Share" -> getFormatShare(tempShare),
                "lastYearYMCompanySales" -> getFormatSales(tempLastYearCompanySalesMap("Sales").toDouble),
                "lastYearYMTotalSales" -> getFormatSales(tempLastYearMaxSalesMap("Sales").toDouble),
                "lastYearYMShare" -> getFormatShare(tempLastYearShare)
            )
        }
    }

}
