package com.pharbers.common.clean

import org.apache.spark.sql.SaveMode
import com.pharbers.pactions.actionbase._

object phSaveCleanAction {
    def apply(args: MapArgs): pActionTrait = new phSaveCleanAction(args)
}

class phSaveCleanAction(override val defaultArgs: MapArgs) extends pActionTrait {
    override val name: String = "phSaveCleanAction"

    override def perform(pr: pActionArgs): pActionArgs = {

        val cleanDF = pr.asInstanceOf[MapArgs].get("phCleanConcretAction").asInstanceOf[DFArgs].get
        val cpa_erd_path = defaultArgs.asInstanceOf[MapArgs].get("cpa_erd_path").asInstanceOf[StringArgs].get
        val cpa_erd_name = defaultArgs.asInstanceOf[MapArgs].get("cpa_erd_name").asInstanceOf[StringArgs].get

        val location = cpa_erd_path + cpa_erd_name

        cleanDF.write.mode(SaveMode.Append)
                .option("header", value = true)
                .parquet(location)

        StringArgs(cpa_erd_name)
    }
}