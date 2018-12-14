package com.pharbers.sparkContexttest

import com.pharbers.pactions.actionbase._
import org.apache.spark.sql.DataFrame

object showJob {
    def apply(args: MapArgs): pActionTrait = new showJob(args)
}

class showJob(override val defaultArgs : pActionArgs) extends pActionTrait {
    override val name: String = "showJob"
    override def perform(args : pActionArgs): pActionArgs = {
        val ym: String = defaultArgs.asInstanceOf[MapArgs].get("ym").asInstanceOf[StringArgs].get
        val univers: DataFrame = defaultArgs.asInstanceOf[MapArgs].get("universe_file").asInstanceOf[DFArgs].get
        univers.show(false)
        DFArgs(univers)
    }
}
