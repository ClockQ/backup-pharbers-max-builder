package com.pharbers.max.nhwa.panel

import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase._

object phNhwaPanelConcretAction {
    def apply(args: MapArgs)(implicit sparkDriver: phSparkDriver): pActionTrait = new phNhwaPanelConcretAction(args)
}

class phNhwaPanelConcretAction(override val defaultArgs: pActionArgs)(implicit sparkDriver: phSparkDriver) extends pActionTrait {
    override val name: String = "phNhwaPanelConcretAction"

    override def perform(args: pActionArgs): pActionArgs = {
        import sparkDriver.ss.implicits._

        val ym = defaultArgs.getAs[StringArgs]("ym")
        val mkt = defaultArgs.getAs[StringArgs]("mkt")

        val cpaData = args.getAs[DFArgs]("cpa_data")
        val missHospData = args.getAs[DFArgs]("miss_hosp_data")
        val notPublishHospData = args.getAs[DFArgs]("not_publish_hosp_data")
        val fullHospData = args.getAs[DFArgs]("full_hosp_data")
        val sampleHospData = args.getAs[DFArgs]("sample_hosp_data")

        // full hosp
        val cpa = {
            val primalCpa = cpaData.filter($"YM" === ym)

            val missHosp = missHospData.filter($"DATE" === ym)
                    .drop("DATE")
                    .unionByName(notPublishHospData)
                    .drop("_id")
                    .distinct()

            val reducedCpa = primalCpa
                    .join(missHosp, primalCpa("HOSPITAL_ID") === missHosp("HOSPITAL_ID"), "left")
                    .filter(missHosp("HOSPITAL_ID").isNull)
                    .drop(missHosp("HOSPITAL_ID"))

            val result = fullHospData.filter($"YM" === ym)
                    .join(missHosp, fullHospData("HOSPITAL_ID") === missHosp("HOSPITAL_ID"))
                    .drop(missHosp("HOSPITAL_ID"))
                    .union(reducedCpa)

            result
        }

        // set sample
        val panelERD = {
            cpa
                    .join(
                        sampleHospData.filter($"MARKET" === mkt).filter($"SAMPLE" === "1")
                        , cpa("HOSPITAL_ID") === sampleHospData("HOSPITAL_ID")
                    )
                    .drop(sampleHospData("HOSPITAL_ID"))
                    .groupBy("COMPANY_ID", "YM", "HOSPITAL_ID", "PRODUCT_ID")
                    .agg(Map("UNITS" -> "sum", "SALES" -> "sum"))
                    .withColumnRenamed("sum(UNITS)", "UNITS")
                    .withColumnRenamed("sum(SALES)", "SALES")
        }

        DFArgs(panelERD)
    }

}