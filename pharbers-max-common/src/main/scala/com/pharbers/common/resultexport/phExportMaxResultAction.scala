package com.pharbers.common.resultexport

import com.pharbers.pactions.actionbase.{pActionTrait, pActionArgs, MapArgs, DFArgs, StringArgs, NULLArgs}

object phExportMaxResultAction {
    def apply(args: pActionArgs = NULLArgs): pActionTrait = new phExportMaxResultAction(args)
}

class phExportMaxResultAction(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "phExportMaxResultAction"

    override def perform(prMap: pActionArgs): pActionArgs = {

        val export_path = defaultArgs.asInstanceOf[MapArgs].get("export_path").asInstanceOf[StringArgs].get
        val export_name = defaultArgs.asInstanceOf[MapArgs].get("export_name").asInstanceOf[StringArgs].get
        val export_delimiter = defaultArgs.asInstanceOf[MapArgs].get("export_delimiter").asInstanceOf[StringArgs].get

        val maxDF = prMap.asInstanceOf[MapArgs].get("phLoadMaxResultAction").asInstanceOf[DFArgs].get

        val exportDataPath = export_path + export_name

        if (maxDF.rdd.isEmpty()) StringArgs("") else {
            maxDF.coalesce(1).write
                    .format("csv")
                    .option("header", value = true)
                    .option("delimiter", export_delimiter)
                    .save(exportDataPath)
            StringArgs(exportDataPath)
        }
    }
}
