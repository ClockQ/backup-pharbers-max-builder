package com.pharbers.common.panel

import com.pharbers.pactions.actionbase._

object phSavePanelJob {
    def apply(args: MapArgs): pActionTrait = new phSavePanelJob(args)
}

class phSavePanelJob(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "phSavePanelJob"

    override def perform(pr: pActionArgs): pActionArgs = {
        val panel = pr.asInstanceOf[MapArgs].get("panel").asInstanceOf[DFArgs].get
        val panel_name = defaultArgs.asInstanceOf[MapArgs].get("panel_name").asInstanceOf[StringArgs].get
        val panel_hdfs_path = defaultArgs.asInstanceOf[MapArgs].get("panel_path").asInstanceOf[StringArgs].get + panel_name

        panel.write.format("csv")
                .option("header", value = true)
                .option("delimiter", 31.toChar.toString)
                .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                .save(panel_hdfs_path)

        StringArgs(panel_name)
    }
}