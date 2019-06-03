package com.pharbers.max.nhwa.max

import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase._

object phNhwaMaxConcretAction {
    def apply(args: MapArgs)(implicit sparkDriver: phSparkDriver): pActionTrait = new phNhwaMaxConcretAction(args)
}

class phNhwaMaxConcretAction(override val defaultArgs: pActionArgs)(implicit sparkDriver: phSparkDriver) extends pActionTrait {
    override val name: String = "phNhwaMaxConcretAction"

    override def perform(args: pActionArgs): pActionArgs = {
        import sparkDriver.ss.implicits._
        import org.apache.spark.sql.functions._

        val panelData = args.getAs[DFArgs]("panel_data")
        val universeData = args.getAs[DFArgs]("universe_hosp_data")

        val universeDF = universeData
                .drop("_id")
                .withColumnRenamed("IF_PANEL_ALL", "IS_PANEL_HOSP")
                .withColumnRenamed("IF_PANEL_TO_USE", "NEED_MAX_HOSP")
                .withColumnRenamed("FACTOR", "FACTOR")
                .withColumnRenamed("WEST_MEDICINE_INCOME", "WEST_MEDICINE_INCOME")

        val panelSummed = panelData.groupBy("YM", "HOSPITAL_ID", "PRODUCT_ID")
                .agg(Map("UNITS" -> "sum", "SALES" -> "sum"))
                .withColumnRenamed("YM", "P_YM")
                .withColumnRenamed("HOSPITAL_ID", "P_HOSPITAL_ID")
                .withColumnRenamed("PRODUCT_ID", "P_PRODUCT_ID")
                .withColumnRenamed("sum(UNITS)", "sumUnits")
                .withColumnRenamed("sum(SALES)", "sumSales")

        val joinDataWithEmptyValue = panelData.select("YM", "PRODUCT_ID").distinct() crossJoin universeDF

        val joinData = {
            joinDataWithEmptyValue
                    .join(
                        panelSummed
                        , joinDataWithEmptyValue("HOSPITAL_ID") === panelSummed("P_HOSPITAL_ID")
                                && joinDataWithEmptyValue("YM") === panelSummed("P_YM")
                                && joinDataWithEmptyValue("PRODUCT_ID") === panelSummed("P_PRODUCT_ID")
                        , "left"
                    )
                    .withColumn("j_sumSales", when($"sumSales".isNull, 0.0).otherwise($"sumSales"))
                    .withColumn("j_sumUnits", when($"sumUnits".isNull, 0.0).otherwise($"sumUnits"))
                    .drop("sumSales", "sumUnits")
                    .withColumnRenamed("j_sumSales", "sumSales")
                    .withColumnRenamed("j_sumUnits", "sumUnits")
        }

        val segmentDF = {
            joinData.filter(col("NEED_MAX_HOSP") === "1")
                    .groupBy("SEGMENT", "PRODUCT_ID", "YM")
                    .agg(Map("sumSales" -> "sum", "sumUnits" -> "sum", "WEST_MEDICINE_INCOME" -> "sum"))
                    .withColumnRenamed("SEGMENT", "s_SEGMENT")
                    .withColumnRenamed("PRODUCT_ID", "s_PRODUCT_ID")
                    .withColumnRenamed("YM", "s_YM")
                    .withColumnRenamed("sum(sumSales)", "s_sumSales")
                    .withColumnRenamed("sum(sumUnits)", "s_sumUnits")
                    .withColumnRenamed("sum(WEST_MEDICINE_INCOME)", "s_westMedicineIncome")
                    .withColumn("avg_Sales", $"s_sumSales" / $"s_westMedicineIncome")
                    .withColumn("avg_Units", $"s_sumUnits" / $"s_westMedicineIncome")
                    .drop("s_sumSales", "s_sumUnits", "s_westMedicineIncome")
        }

        val enlargedDF = {
            joinData
                    .join(segmentDF,
                        joinData("SEGMENT") === segmentDF("s_SEGMENT")
                                && joinData("PRODUCT_ID") === segmentDF("s_PRODUCT_ID")
                                && joinData("YM") === segmentDF("s_YM")
                    )
                    .drop("s_SEGMENT", "s_PRODUCT_ID", "s_YM")
                    .withColumn("FACTOR", $"FACTOR".cast("double"))
                    .filter("FACTOR > 0")
                    .withColumn("f_sales",
                        when($"IS_PANEL_HOSP" === 1, $"sumSales").otherwise(
                            when($"avg_Sales" <= 0.0 or $"avg_Units" <= 0.0, 0.0)
                                    .otherwise($"Factor" * $"avg_Sales" * $"WEST_MEDICINE_INCOME")
                        ).cast("double"))
                    .withColumn("f_units",
                        when($"IS_PANEL_HOSP" === 1, $"sumUnits").otherwise(
                            when($"avg_Sales" <= 0.0 or $"avg_Units" <= 0.0, 0.0)
                                    .otherwise($"Factor" * $"avg_Units" * $"WEST_MEDICINE_INCOME")
                        ).cast("double"))
                    .drop("s_sumSales", "s_sumUnits", "s_westMedicineIncome")
                    .withColumn("flag",
                        when($"IS_PANEL_HOSP" === 1, 1).otherwise(
                            when($"f_units" === 0 and $"f_sales" === 0, 0).otherwise(1)
                        ))
                    .filter($"flag" === 1 && $"IS_PANEL_HOSP" === 0)
                    .withColumn("Date", 'YM.cast("int"))
                    .select("Date", "HOSPITAL_ID", "PRODUCT_ID", "f_units", "f_sales")
        }

        val backfillDF = {
            panelData.join(universeDF, panelData("HOSPITAL_ID") === universeDF("HOSPITAL_ID"))
                    .drop(panelData("HOSPITAL_ID"))
                    .withColumn("Date", 'YM.cast("int"))
                    .withColumnRenamed("Sales", "f_sales")
                    .withColumnRenamed("Units", "f_units")
                    .select("Date", "HOSPITAL_ID", "PRODUCT_ID", "f_units", "f_sales")
        }

        val maxDF = backfillDF.unionByName(enlargedDF)

        DFArgs(maxDF)
    }

}