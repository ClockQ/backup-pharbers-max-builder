package com.pharbers.astellas.calcym

import com.pharbers.pactions.actionbase._
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, when}
import org.apache.spark.sql.types.{IntegerType, LongType}

import scala.reflect.ClassTag

object phAstellasCountYm {
    def apply[T: ClassTag](args: pActionArgs = NULLArgs): pActionTrait = {
        new phAstellasCountYm[T](args)
    }
}

class phAstellasCountYm[T: ClassTag](override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "calcYm"

    override def perform(prMap: pActionArgs): pActionArgs = {

        val job_id = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get
        val sparkDriver: phSparkDriver = phSparkDriver(job_id)

        val cpaDf = prMap.asInstanceOf[MapArgs].get("cpa").asInstanceOf[DFArgs].get
        val cpaCountDf = countYm(cpaDf)(sparkDriver)
        val cpaResult = calcYm(cpaCountDf)(sparkDriver)

        val gycxDf = prMap.asInstanceOf[MapArgs].get("gycx").asInstanceOf[DFArgs].get
        val gycxCountDf = countYm(gycxDf)(sparkDriver)
        val gycxResult = calcYm(gycxCountDf)(sparkDriver)

        val result = cpaResult.toSet & gycxResult.toSet
        ListArgs(result.map(StringArgs).toList)
    }

    def countYm(df: DataFrame)(driver: phSparkDriver): DataFrame = {
        df.select("YM", "HOSP_ID")
                .groupBy("YM")
                .count()
    }

    def calcYm(df: DataFrame)(driver: phSparkDriver): Array[String] = {
        import driver.ss.implicits._
        val maxYm = df.agg(Map("count" -> "max")).collect().head.getLong(0)
        df.withColumn("count", 'count.cast(LongType))
                .filter(s"count > $maxYm/2")
                .select("ym")
                .collect()
                .map(_.getString(0))
                .sorted
    }
}