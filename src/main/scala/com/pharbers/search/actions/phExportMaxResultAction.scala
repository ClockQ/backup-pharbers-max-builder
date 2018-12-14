package com.pharbers.search.actions

import java.util.{Date, UUID}

import com.pharbers.common.algorithm.{max_path_obj, phSparkCommonFuncTrait}
import com.pharbers.pactions.actionbase._

object phExportMaxResultAction {
    def apply(args: pActionArgs = NULLArgs): pActionTrait = new phExportMaxResultAction(args)
}

class phExportMaxResultAction(override val defaultArgs: pActionArgs) extends pActionTrait with phSparkCommonFuncTrait {
    override val name: String = "export_max_result_action"

    override def perform(pr: pActionArgs): pActionArgs = {

        val maxDF = pr.asInstanceOf[MapArgs].get("read_max_result_action").asInstanceOf[DFArgs].get
        val maxSearchResultName = UUID.randomUUID().toString
        val exportDataPath = max_path_obj.p_exportPath + maxSearchResultName

        val result =
            if (maxDF.rdd.isEmpty()) StringArgs("")
            else {
                maxDF
                    .coalesce(1).write
                    .format("csv")
                    .option("header", value = true)
                    .option("delimiter", 31.toChar.toString)
                    .save(exportDataPath)
                StringArgs(exportDataPath)
            }
        result
    }

}
