package com.pharbers.panel.nhwa

import scala.reflect.ClassTag
import com.pharbers.pactions.actionbase._
import com.pharbers.panel.format.input.writable.nhwa.phNhwaCpaWritable
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.functions.{col, concat, trim, when}
import org.apache.spark.sql.types.IntegerType

object phNhwaCalcYMConcretJob {
    def apply[T : ClassTag](args: pActionArgs = NULLArgs): pActionTrait = {
        new phNhwaCalcYMConcretJob[T](args)
    }
}

class phNhwaCalcYMConcretJob[T : ClassTag](override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "calcYM"
    override def perform(pr : pActionArgs): pActionArgs = {
        val job_id = defaultArgs.asInstanceOf[StringArgs].get
        lazy val sparkDriver: phSparkDriver = phSparkDriver(job_id)
        import sparkDriver.ss.implicits._
        val cpaDF = pr.asInstanceOf[MapArgs].get("cpa").asInstanceOf[DFArgs].get
                .na.fill(value = "0", cols = Array("VALUE", "STANDARD_UNIT"))
                .withColumn("PRODUCT_NAME", when(col("PRODUCT_NAME").isNull, col("MOLE_NAME"))
                        .otherwise(col("PRODUCT_NAME")))
                .withColumn("MONTH", 'MONTH.cast(IntegerType))
                .withColumn("MONTH", when(col("MONTH").>=(10), col("MONTH"))
                        .otherwise(concat(col("MONTH").*(0).cast("int"), col("MONTH"))))
                .withColumn("PRODUCT_NAME", trim(col("PRODUCT_NAME")))
                .withColumn("DOSAGE", trim(col("DOSAGE")))
                .withColumn("PACK_DES", trim(col("PACK_DES")))
                .withColumn("PACK_NUMBER", trim(col("PACK_NUMBER")))
                .withColumn("CORP_NAME", trim(col("CORP_NAME")))
                .withColumn("min1", concat(col("PRODUCT_NAME"), col("DOSAGE"), col("PACK_DES"), col("PACK_NUMBER"), col("CORP_NAME")))
                .withColumn("ym", concat(col("YEAR"), col("MONTH")))

        val ymResult = cpaDF.select("ym", "HOSP_ID")
                .groupBy("ym")
                .count()
        DFArgs(ymResult)
    }
}