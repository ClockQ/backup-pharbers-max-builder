package com.pharbers.builder.search

import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.search.phExportMaxResultJob
import com.pharbers.spark.phSparkDriver

/**
  * @ ProjectName pharbers-max.com.pharbers.builder.search.ExportMaxResult
  * @ author jeorch
  * @ date 18-10-15
  * @ Description: TODO
  */
class MaxResultFacade {

    def export(company: String, ym: String, mkt: String, job_id: String): String ={
        val args = Map(
            "company" -> company,
            "ym" -> ym,
            "mkt" -> mkt,
            "job_id" -> job_id
        )
        val exportResult =  phExportMaxResultJob(args).perform().asInstanceOf[MapArgs].get("export_max_result_action").asInstanceOf[StringArgs].get
//        phSparkDriver(job_id).sc.stop
        phSparkDriver(job_id).stopCurrConn
        exportResult
    }

}
