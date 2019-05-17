package com.pharbers.nhwa2.calcym

import com.pharbers.pactions.actionbase._
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, when}
import org.apache.spark.sql.types.{IntegerType, LongType}

import scala.reflect.ClassTag

object phNhwaCountYm {
    def apply[T: ClassTag](args: pActionArgs = NULLArgs): pActionTrait = {
        new phNhwaCountYm[T](args)
    }
}

class phNhwaCountYm[T: ClassTag](override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "calcYm"

    override def perform(prMap: pActionArgs): pActionArgs = {

        val job_id = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get
        val sparkDriver: phSparkDriver = phSparkDriver(job_id)

        val cpaDf = prMap.asInstanceOf[MapArgs].get("cpa").asInstanceOf[DFArgs].get
        val countDf = countYm(cpaDf)(sparkDriver)
        val result = calcYm(countDf)(sparkDriver)

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
                .select("YM")
                .collect()
                .map(_.getString(0))
                .sorted
    }
}