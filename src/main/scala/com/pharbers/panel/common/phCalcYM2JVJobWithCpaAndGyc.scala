package com.pharbers.panel.common

import play.api.libs.json.Json.toJson
import com.pharbers.pactions.actionbase._
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.types.LongType

object phCalcYM2JVJobWithCpaAndGyc  {
    def apply(args: pActionArgs = NULLArgs) : pActionTrait = new phCalcYM2JVJobWithCpaAndGyc(args)
}

class phCalcYM2JVJobWithCpaAndGyc(override val defaultArgs: pActionArgs) extends pActionTrait {

    override val name: String = "result"
    override def perform(prMap : pActionArgs): pActionArgs = {

        val job_id = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get
        lazy val sparkDriver: phSparkDriver = phSparkDriver(job_id)
        import sparkDriver.ss.implicits._
        val cpa = prMap.asInstanceOf[MapArgs].get("calcYMWithCpa").asInstanceOf[DFArgs].get
        val cpaMax = cpa.agg(Map("count" -> "max")).collect().head.getLong(0)
        val cpaResult = cpa.withColumn("count", 'count.cast(LongType))
            .filter(s"count > $cpaMax/2")
            .select("ym")
            .collect()
            .map(_.getString(0))
            .sorted

        val gycx = prMap.asInstanceOf[MapArgs].get("calcYMWithGycx").asInstanceOf[DFArgs].get
        val gycxMax = gycx.agg(Map("count" -> "max")).collect().head.getLong(0)
        val gycxResult = gycx.withColumn("count", 'count.cast(LongType))
            .filter(s"count > $gycxMax/2")
            .select("ym")
            .collect()
            .map(_.getString(0))
            .sorted

        val result = cpaResult.toSet & gycxResult.toSet

        JVArgs(
            toJson(result.mkString("#"))
        )
    }
}