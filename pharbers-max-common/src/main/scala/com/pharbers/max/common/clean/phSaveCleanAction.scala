package com.pharbers.max.common.clean

import org.apache.spark.sql.SaveMode
import com.pharbers.pactions.actionbase._

object phSaveCleanAction {
    def apply(args: MapArgs, saveKey: String = "phCleanConcretAction",
              name: String = "phSaveCleanAction"): pActionTrait =
        new phSaveCleanAction(name, args, saveKey)
}

class phSaveCleanAction(override val name: String,
                        override val defaultArgs: MapArgs,
                        saveKey: String) extends pActionTrait {

    override def perform(pr: pActionArgs): pActionArgs = {

        val cleanDF = pr.asInstanceOf[MapArgs].get(saveKey).asInstanceOf[DFArgs].get
        val cpa_erd_path = defaultArgs.asInstanceOf[MapArgs].get("cpa_erd_path").asInstanceOf[StringArgs].get
        val cpa_erd_name = defaultArgs.asInstanceOf[MapArgs].get("cpa_erd_name").asInstanceOf[StringArgs].get

        val location = cpa_erd_path + cpa_erd_name

        cleanDF.write.mode(SaveMode.Append)
                .option("header", value = true)
                .parquet(location)

        StringArgs(location)
    }
}