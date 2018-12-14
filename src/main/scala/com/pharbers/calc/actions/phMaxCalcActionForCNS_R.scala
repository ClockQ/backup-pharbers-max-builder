package com.pharbers.calc.actions

import com.pharbers.pactions.actionbase._
import com.pharbers.spark.phSparkDriver
import com.sun.jdi
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

/**
  * Created by jeorch on 18-5-3.
  */
object phMaxCalcActionForCNS_R {
    def apply(args: pActionArgs = NULLArgs): pActionTrait = new phMaxCalcActionForCNS_R(args)
}

class phMaxCalcActionForCNS_R(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "max_calc_action"

    override def perform(pr: pActionArgs): pActionArgs = {

        val job_id = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get
        lazy val sparkDriver: phSparkDriver = phSparkDriver(job_id)
        import sparkDriver.ss.implicits._

        val panelDF = {
            pr.asInstanceOf[MapArgs].get("panel_data").asInstanceOf[DFArgs].get
                .withColumnRenamed("Date", "YM")
                .withColumnRenamed("Strength", "min1")
                .withColumnRenamed("DOI", "MARKET")
                .selectExpr("YM", "min1", "HOSP_ID", "Sales", "Units", "MARKET")
        }

        val universeDF = {
            pr.asInstanceOf[MapArgs].get("universe_data").asInstanceOf[DFArgs].get
                .withColumnRenamed("PHA_HOSP_ID", "PHA_ID")
                .withColumnRenamed("IF_PANEL_ALL", "IS_PANEL_HOSP")
                .withColumnRenamed("IF_PANEL_TO_USE", "NEED_MAX_HOSP")
                .withColumnRenamed("WEST_MEDICINE_INCOME", "westMedicineIncome")
                .withColumnRenamed("FACTOR1", "Factor1")
                .withColumnRenamed("FACTOR2", "Factor2")
                .withColumnRenamed("PROVINCE", "Province")
                .withColumnRenamed("PREFECTURE", "Prefecture")
                .selectExpr("PHA_ID", "Factor1", "Factor2", "IS_PANEL_HOSP", "NEED_MAX_HOSP", "SEGMENT", "Province", "Prefecture", "westMedicineIncome")
        }

        val panelSummed = {
            panelDF.groupBy("YM", "min1", "HOSP_ID")
                .agg(Map("Units" -> "sum", "Sales" -> "sum"))
                .withColumnRenamed("YM", "sumYM")
                .withColumnRenamed("min1", "sumMin1")
                .withColumnRenamed("HOSP_ID", "sumHosp_ID")
                .withColumnRenamed("sum(Sales)", "sumSales")
                .withColumnRenamed("sum(Units)", "sumUnits")
        }

        val joinDataWithEmptyValue = panelDF.select("YM", "min1", "MARKET").distinct() join universeDF

        val joinData = {
            joinDataWithEmptyValue
                .join(panelSummed,
                    joinDataWithEmptyValue("PHA_ID") === panelSummed("sumHosp_ID")
                        && joinDataWithEmptyValue("YM") === panelSummed("sumYM")
                        && joinDataWithEmptyValue("min1") === panelSummed("sumMin1"),
                    "left")
                .withColumn("j_sumSales", when($"sumSales".isNull, 0.0).otherwise($"sumSales"))
                .withColumn("j_sumUnits", when($"sumUnits".isNull, 0.0).otherwise($"sumUnits"))
                .drop("sumSales", "sumUnits")
                .withColumnRenamed("j_sumSales", "sumSales")
                .withColumnRenamed("j_sumUnits", "sumUnits")
        }

        val segmentDF = {
            joinData.filter(col("NEED_MAX_HOSP") === "1")
                .groupBy("SEGMENT", "min1", "YM")
                .agg(Map("sumSales" -> "sum", "sumUnits" -> "sum", "westMedicineIncome" -> "sum"))
                .withColumnRenamed("SEGMENT", "s_SEGMENT")
                .withColumnRenamed("min1", "s_min1")
                .withColumnRenamed("YM", "s_YM")
                .withColumnRenamed("sum(sumSales)", "s_sumSales")
                .withColumnRenamed("sum(sumUnits)", "s_sumUnits")
                .withColumnRenamed("sum(westMedicineIncome)", "s_westMedicineIncome")
                .withColumn("avg_Sales", $"s_sumSales" / $"s_westMedicineIncome")
                .withColumn("avg_Units", $"s_sumUnits" / $"s_westMedicineIncome")
                .drop("s_sumSales", "s_sumUnits", "s_westMedicineIncome")
        }

        val enlargedDF = {
            joinData
                .join(segmentDF,
                    joinData("SEGMENT") === segmentDF("s_SEGMENT")
                        && joinData("min1") === segmentDF("s_min1")
                        && joinData("YM") === segmentDF("s_YM"))
                .drop("s_SEGMENT", "s_min1", "s_YM")
                .withColumn("Factor", when($"min1" like "%粉针剂%", $"Factor1")
                    .otherwise(when($"min1" like "%注射剂%", $"Factor1")
                        .otherwise($"Factor2")))
                .withColumn("Factor", 'Factor.cast(DoubleType))
                .filter("Factor > 0")
                .withColumn("f_sales",
                    when($"IS_PANEL_HOSP" === 1, $"sumSales").otherwise(
                        when($"avg_Sales" <= 0.0 or $"avg_Units" <= 0.0, 0.0)
                            .otherwise($"Factor" * $"avg_Sales" * $"westMedicineIncome")
                    ).cast(DoubleType))
                .withColumn("f_units",
                    when($"IS_PANEL_HOSP" === 1, $"sumUnits").otherwise(
                        when($"avg_Sales" < 0.0 or $"avg_Units" < 0.0, 0.0)
                            .otherwise($"Factor" * $"avg_Units" * $"westMedicineIncome")
                    ).cast(DoubleType))
                .drop("s_sumSales", "s_sumUnits", "s_westMedicineIncome")
                .withColumn("flag",
                    when($"IS_PANEL_HOSP" === 1, 1).otherwise(
                        when($"f_units" === 0 and $"f_sales" === 0, 0).otherwise(1)
                    ))
                .filter($"flag" === 1 && $"IS_PANEL_HOSP" === 0)
                .withColumn("Date", 'YM.cast(IntegerType))
                .withColumnRenamed("PHA_ID", "Panel_ID")
                .withColumnRenamed("Prefecture", "City")
                .withColumnRenamed("min1", "Product")
                .select("Date", "Province", "City", "Panel_ID", "Product", "Factor", "f_sales", "f_units", "MARKET")
        }

        val backfillDF = {
            panelDF.join(universeDF, panelDF("Hosp_ID") === universeDF("PHA_ID"))
                .withColumn("Date", 'YM.cast(IntegerType))
                .withColumnRenamed("Prefecture", "City")
                .withColumnRenamed("PHA_ID", "Panel_ID")
                .withColumnRenamed("Sales", "f_sales")
                .withColumnRenamed("Units", "f_units")
                .withColumn("Factor", when($"min1" like "%粉针剂%", $"Factor1")
                    .otherwise(when($"min1" like "%注射剂%", $"Factor1")
                        .otherwise($"Factor2")))
                .withColumnRenamed("min1", "Product")
                .selectExpr("Date", "Province", "City", "Panel_ID", "Product", "Factor", "f_sales", "f_units", "MARKET")
        }

        DFArgs(backfillDF.union(enlargedDF))
    }

}
