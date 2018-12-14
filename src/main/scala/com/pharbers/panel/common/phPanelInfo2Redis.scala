package com.pharbers.panel.common

import java.util.Base64

import com.pharbers.driver.PhRedisDriver
import com.pharbers.pactions.actionbase._
import com.pharbers.sercuity.Sercurity
import com.pharbers.builder.phMarketTable.phMarketManager
import com.pharbers.common.algorithm.alTempLog

object phPanelInfo2Redis {
    def apply(args: MapArgs): pActionTrait = new phPanelInfo2Redis(args)
}

class phPanelInfo2Redis(override val defaultArgs: pActionArgs) extends pActionTrait with phMarketManager {
    override val name: String = "phPanelInfo2Redis"
    
    override def perform(pr: pActionArgs): pActionArgs = {
        val panelDF = pr.asInstanceOf[MapArgs].get("panel").asInstanceOf[DFArgs].get
        val hosp_ID_file = pr.asInstanceOf[MapArgs].get("hosp_ID_file").asInstanceOf[DFArgs].get
                .withColumnRenamed("PHA_HOSP_ID", "u_HOSP_ID")
                .withColumnRenamed("PHA_HOSP_NAME", "HOSP_NAME")
                .withColumnRenamed("IF_PANEL_ALL", "SAMPLE")
                .filter("SAMPLE like '1'")
                .select("u_HOSP_ID", "HOSP_NAME")
        
        val company = defaultArgs.asInstanceOf[MapArgs].get("company").asInstanceOf[StringArgs].get
        val user = defaultArgs.asInstanceOf[MapArgs].get("user").asInstanceOf[StringArgs].get
        val ym = defaultArgs.asInstanceOf[MapArgs].get("ym").asInstanceOf[StringArgs].get
        val mkt = defaultArgs.asInstanceOf[MapArgs].get("mkt").asInstanceOf[StringArgs].get
        val panel_name = defaultArgs.asInstanceOf[MapArgs].get("name").asInstanceOf[StringArgs].get
        val job_id = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get
        
        val condition = getAllSubsidiary(company).map(x => s"Prod_Name like '%$x%'").mkString(" OR ") //获得所有子公司
        val panelDF_filter_company = panelDF.filter(condition) // 包含子公司关键字的数据
        val panel_hosp_distinct = panelDF.withColumnRenamed("HOSP_ID", "p_HOSP_ID").select("p_HOSP_ID").distinct()

        alTempLog(s"准备 -- 存储${mkt}市场的[panel]聚合数据到Redis中")

        val panel_prod_count = panelDF.select("Prod_Name").distinct().count()
        val panel_sales = panelDF.agg(Map("Sales" -> "sum")).take(1)(0).toString().split('[').last.split(']').head.toDouble
        val panel_company_sales = if (panelDF_filter_company.count() == 0) 0.0
        else panelDF_filter_company.agg(Map("Sales" -> "sum")).take(1)(0).toString().split('[').last.split(']').head.toDouble
        val not_panel_hosp_lst = hosp_ID_file
                .join(panel_hosp_distinct, hosp_ID_file("u_HOSP_ID") === panel_hosp_distinct("p_HOSP_ID"), "left")
                .filter("p_HOSP_ID is null").select("HOSP_NAME").collect().map(x => x.toString())

        alTempLog(s"开始 -- 存储${mkt}市场的[panel]聚合数据到Redis中")

        val rd = new PhRedisDriver()
        //TODO:singleJobKey的加密改为Base64(company + ym + mkt)，同一公司下的所有用户可以看到彼此的保存历史
        val singleJobKey = Base64.getEncoder.encodeToString((company + "#" + ym + "#" + mkt).getBytes())
        
        //TODO:SinglePanelSpecialKey for example -> not_panel_hosp_key it depends on (user + company + ym + mkt) but had same key
        val not_panel_hosp_key = Sercurity.md5Hash(user + company + ym + mkt + "not_panel_hosp_lst")
        
        rd.addSet(job_id, panel_name)
        rd.addSet(job_id + "ym", ym)
        rd.expire(job_id, 60 * 60 * 24)
        rd.expire(job_id + "ym", 60 * 60 * 24)
        
        rd.addMap(panel_name, "ym", ym)
        rd.addMap(panel_name, "mkt", mkt)
        rd.expire(panel_name, 60 * 60 * 24)
        
        rd.addMap(singleJobKey, "panel_hosp_count", panel_hosp_distinct.count())
        rd.addMap(singleJobKey, "panel_prod_count", panel_prod_count)
        rd.addMap(singleJobKey, "panel_sales", panel_sales)
        rd.addMap(singleJobKey, "panel_company_sales", panel_company_sales)
        rd.expire(singleJobKey, 60*60*24)
        
        rd.delete(not_panel_hosp_key)
        rd.addSet(not_panel_hosp_key, not_panel_hosp_lst: _*)
        rd.expire(not_panel_hosp_key, 60*60*24)

        alTempLog(s"完成 -- 存储${mkt}市场的[panel]聚合数据到Redis中")

        StringArgs(singleJobKey)
    }
    
}