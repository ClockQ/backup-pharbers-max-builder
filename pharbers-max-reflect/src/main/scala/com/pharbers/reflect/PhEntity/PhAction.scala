package com.pharbers.reflect.PhEntity

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.{One2ManyConn, One2OneConn, ToStringMacro}

@One2OneConn[PhXmppConf]("xmppConf")
@One2OneConn[PhCalcYmConf]("calcYmConf")
@One2ManyConn[PhPanelConf]("panelConf")
@One2ManyConn[PhCalcConf]("calcConf")
@One2ManyConn[PhUnitTestConf]("unitTestConf")
@ToStringMacro
class PhAction extends commonEntity {
    var job_id: String = ""
    var user_id: String = ""
    var company_id: String = ""

    var panel_path: String = ""
    var max_path: String = ""
    var prod_lst: String = ""

    def ckElem(value: String): String = {
        if(value.isEmpty) throw new Exception("element is none")
        value
    }

    def ymArgs(ymConf: PhCalcYmConf): Map[String, String] = {
        Map(
            "job_id" -> ckElem(job_id),
            "user_id" -> ckElem(user_id),
            "company_id" -> ckElem(company_id)
        ) ++ ymConf.conf
    }

    def panelArgs(p_current: Int, p_total: Int)
                 (panelConf: PhPanelConf): Map[String, String] = {
        Map(
            "job_id" -> ckElem(job_id),
            "user_id" -> ckElem(user_id),
            "company_id" -> ckElem(company_id),
            "panel_path" -> ckElem(panel_path),
            "prod_lst" -> ckElem(prod_lst),
            "ym" -> ckElem(panelConf.ym),
            "mkt" -> ckElem(panelConf.mkt),
            "panel_name" -> ckElem(panelConf.panel_name),
            "p_current" -> ckElem(p_current.toString),
            "p_total" -> ckElem(p_total.toString)
        ) ++ panelConf.conf
    }

    def calcArgs(p_current: Int, p_total: Int)
                (calcConf: PhCalcConf): Map[String, String] = {
        Map(
            "job_id" -> ckElem(job_id),
            "user_id" -> ckElem(user_id),
            "company_id" -> ckElem(company_id),
            "panel_path" -> ckElem(panel_path),
            "max_path" -> ckElem(max_path),
            "prod_lst" -> ckElem(prod_lst),
            "ym" -> ckElem(calcConf.ym),
            "mkt" -> ckElem(calcConf.mkt),
            "panel_name" -> ckElem(calcConf.panel_name),
            "max_name" -> ckElem(calcConf.max_name),
            "max_search_name" -> ckElem(calcConf.max_search_name),
            "p_current" -> ckElem(p_current.toString),
            "p_total" -> ckElem(p_total.toString)
        ) ++ calcConf.conf
    }
}
