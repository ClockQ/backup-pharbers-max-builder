package com.pharbers.search.actions

import java.util.Base64

import com.pharbers.common.algorithm.{max_path_obj, phSparkCommonFuncTrait}
import com.pharbers.driver.PhRedisDriver
import com.pharbers.pactions.actionbase._
import com.pharbers.spark.phSparkDriver

object phReadMaxResultAction {
    def apply(args: pActionArgs = NULLArgs): pActionTrait = new phReadMaxResultAction(args)
}

class phReadMaxResultAction (override val defaultArgs: pActionArgs) extends pActionTrait with phSparkCommonFuncTrait {
    override val name: String = "read_max_result_action"

    override def perform(pr: pActionArgs): pActionArgs = {


        val job_id = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get
        val company = defaultArgs.asInstanceOf[MapArgs].get("company").asInstanceOf[StringArgs].get
        val mkt = defaultArgs.asInstanceOf[MapArgs].get("mkt").asInstanceOf[StringArgs].get
        val ym = defaultArgs.asInstanceOf[MapArgs].get("ym").asInstanceOf[StringArgs].get
        val singleJobKey = Base64.getEncoder.encodeToString((company +"#"+ ym +"#"+ mkt).getBytes())

        val sparkDriver = phSparkDriver(job_id)
        val rd = new PhRedisDriver()

        val maxName = rd.getMapValue(singleJobKey, "max_result_name")
        val maxDF = sparkDriver.readCsv(max_path_obj.p_maxPath + maxName, 31.toChar.toString)

        DFArgs(maxDF)
    }
}
