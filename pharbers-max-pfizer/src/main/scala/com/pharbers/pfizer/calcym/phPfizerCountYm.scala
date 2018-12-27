package com.pharbers.pfizer.calcym

import scala.reflect.ClassTag
import org.apache.spark.sql.DataFrame
import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase._
import org.apache.spark.sql.functions.{col, concat, when}
import org.apache.spark.sql.types.{IntegerType, LongType}

object phPfizerCountYm {
    def apply[T: ClassTag](args: pActionArgs = NULLArgs): pActionTrait = {
        new phPfizerCountYm[T](args)
    }
}

class phPfizerCountYm[T: ClassTag](override val defaultArgs: pActionArgs) extends pActionTrait {
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
        import driver.ss.implicits._
        df.withColumn("MONTH", 'MONTH.cast(IntegerType))
                .withColumn("MONTH", when(col("MONTH").>=(10), col("MONTH"))
                        .otherwise(concat(col("MONTH").*(0).cast("int"), col("MONTH"))))
                .withColumn("YM", concat(col("YEAR"), col("MONTH")))
                .select("YM", "HOSP_ID")
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