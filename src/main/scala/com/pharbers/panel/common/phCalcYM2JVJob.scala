package com.pharbers.panel.common

import scala.reflect.ClassTag
import play.api.libs.json.Json.toJson
import com.pharbers.pactions.actionbase._
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.LongType

object phCalcYM2JVJob {
    def apply[T: ClassTag](args: pActionArgs = NULLArgs): pActionTrait = {
        new phCalcYM2JVJob[T](args)
    }
}

class phCalcYM2JVJob[T: ClassTag](override val defaultArgs: pActionArgs) extends pActionTrait {
    
    override val name: String = "result"
    
    override def perform(pr: pActionArgs): pActionArgs = {
        val job_id = defaultArgs.asInstanceOf[StringArgs].get
        lazy val sparkDriver: phSparkDriver = phSparkDriver(job_id)
        import sparkDriver.ss.implicits._
        val calcYM = pr.asInstanceOf[MapArgs].get("calcYM").asInstanceOf[DFArgs].get
        val maxYm = calcYM.agg(Map("count" -> "max")).collect().head.getLong(0)
        val result = calcYM.withColumn("count", 'count.cast(LongType))
                .filter(s"count > $maxYm/2")
                .select("ym")
                        .collect()
                        .map(_.getString(0))
                        .sorted
        JVArgs(toJson(result.mkString("#")))
    }
}