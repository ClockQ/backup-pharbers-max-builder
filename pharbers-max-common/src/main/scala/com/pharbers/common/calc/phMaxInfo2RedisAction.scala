package com.pharbers.common.calc

import java.util.Base64

import com.pharbers.driver.PhRedisDriver
import com.pharbers.pactions.actionbase._
import com.pharbers.security.cryptogram.md5.md5
import com.pharbers.util.log.phLogTrait

object phMaxInfo2RedisAction {
    def apply(args: pActionArgs = NULLArgs): pActionTrait = new phMaxInfo2RedisAction(args)
}

class phMaxInfo2RedisAction(override val defaultArgs: pActionArgs) extends pActionTrait with phLogTrait {
    override val name: String = "phMaxInfo2RedisAction"
    override def perform(prMap: pActionArgs): pActionArgs = {
        val rd = new PhRedisDriver()
        val md5 = new md5()

        val ym = defaultArgs.asInstanceOf[MapArgs].get("ym").asInstanceOf[StringArgs].get
        val mkt = defaultArgs.asInstanceOf[MapArgs].get("mkt").asInstanceOf[StringArgs].get
        val company_id = defaultArgs.asInstanceOf[MapArgs].get("company_id").asInstanceOf[StringArgs].get
        val prod_name = defaultArgs.asInstanceOf[MapArgs].get("prod_name").asInstanceOf[ListArgs].get.map(_.asInstanceOf[StringArgs].get)
        val max_name = defaultArgs.asInstanceOf[MapArgs].get("max_name").asInstanceOf[StringArgs].get
        val max_search_name = defaultArgs.asInstanceOf[MapArgs].get("max_search_name").asInstanceOf[StringArgs].get

        val maxDF = prMap.asInstanceOf[MapArgs].get("max_calc_action").asInstanceOf[DFArgs].get

        val singleJobKey = Base64.getEncoder.encodeToString((company_id +"#"+ ym +"#"+ mkt).getBytes())

        val max_sales_city_lst_key = md5.encrypt(company_id + ym + mkt + "max_sales_city_lst_key")
        val max_sales_prov_lst_key = md5.encrypt(company_id + ym + mkt + "max_sales_prov_lst_key")
        val company_sales_city_lst_key = md5.encrypt(company_id + ym + mkt + "company_sales_city_lst_key")
        val company_sales_prov_lst_key = md5.encrypt(company_id + ym + mkt + "company_sales_prov_lst_key")

        rd.delete(
            max_sales_city_lst_key,
            max_sales_prov_lst_key,
            company_sales_city_lst_key,
            company_sales_prov_lst_key
        )

        // Save按钮就下面三句话
        val maxSingleDayJobsKey = md5.encrypt("Pharbers")
        rd.addSet(maxSingleDayJobsKey, singleJobKey)
        rd.expire(maxSingleDayJobsKey, 60*60*24)

        phInfoLog(s"准备 -- 存储${mkt}市场的calc聚合数据到Redis中")

        val max_sales = maxDF.agg(Map("f_sales" -> "sum")).take(1)(0).getDouble(0)
        val max_sales_city_lst = maxDF.groupBy("City").agg(Map("f_sales" -> "sum")).sort("sum(f_sales)")
            .collect().map(x => x.toString())
        val max_sales_prov_lst = maxDF.groupBy("Province").agg(Map("f_sales" -> "sum")).sort("sum(f_sales)")
            .collect().map(x => x.toString())

        val condition = prod_name.map(x => s"Product like '%$x%'").mkString(" OR ") //获得所有子公司
        val maxDF_filter_company = maxDF.filter(condition)
        val max_company_sales = if (maxDF_filter_company.count() == 0) 0.0
                                else maxDF_filter_company.agg(Map("f_sales" -> "sum")).take(1)(0).getDouble(0)

        val company_sales_city_lst = maxDF_filter_company.groupBy("City").agg(Map("f_sales" -> "sum")).sort("sum(f_sales)")
            .collect().map(x => x.toString())
        val company_sales_prov_lst = maxDF_filter_company.groupBy("Province").agg(Map("f_sales" -> "sum")).sort("sum(f_sales)")
            .collect().map(x => x.toString())

//        maxDF.groupBy("Date", "Province", "City", "MARKET", "Product")
////            .agg(Map("f_sales"->"sum", "f_units"->"sum", "Panel_ID"->"first"))
//            .agg(Map("f_sales"->"sum", "f_units"->"sum"))
//            .write
//            .format("csv")
//            .option("header", value = true)
//            .option("delimiter", 31.toChar.toString)
//            .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
//            .save(max_path_obj.p_maxPath + maxNameForSearch)

        phInfoLog(s"开始 -- 存储${mkt}市场的calc聚合数据到Redis中")

        rd.addMap(singleJobKey, "max_result_name", max_name)
        rd.addMap(singleJobKey, "max_result_name_for_search", max_search_name)
        rd.addMap(singleJobKey, "max_sales", max_sales)
        rd.addMap(singleJobKey, "max_company_sales", max_company_sales)
        rd.addListLeft(max_sales_city_lst_key, max_sales_city_lst:_*)
        rd.addListLeft(max_sales_prov_lst_key, max_sales_prov_lst:_*)
        rd.addListLeft(company_sales_city_lst_key, company_sales_city_lst:_*)
        rd.addListLeft(company_sales_prov_lst_key, company_sales_prov_lst:_*)

        rd.expire(max_sales_city_lst_key, 60*60*24)
        rd.expire(max_sales_prov_lst_key, 60*60*24)
        rd.expire(company_sales_city_lst_key, 60*60*24)
        rd.expire(company_sales_prov_lst_key, 60*60*24)

        phInfoLog(s"完成 -- 存储${mkt}市场的calc聚合数据到Redis中")

        StringArgs(singleJobKey)
    }
}