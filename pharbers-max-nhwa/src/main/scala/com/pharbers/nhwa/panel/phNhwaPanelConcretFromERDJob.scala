package com.pharbers.nhwa.panel

import com.pharbers.data.conversion.{CPAConversion, ProductEtcConversion}
import com.pharbers.data.util.commonUDF
import com.pharbers.pactions.actionbase._
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.apache.spark.sql.functions._

object phNhwaPanelConcretFromERDJob {
    def apply(args: MapArgs): pActionTrait = new phNhwaPanelConcretFromERDJob(args)
}

class phNhwaPanelConcretFromERDJob(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "panel"

    override def perform(args: pActionArgs): pActionArgs = {
        val ym = defaultArgs.asInstanceOf[MapArgs].get("ym").asInstanceOf[StringArgs].get
        val mkt = defaultArgs.asInstanceOf[MapArgs].get("mkt").asInstanceOf[StringArgs].get
        val job_id = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get
        val company_id = defaultArgs.asInstanceOf[MapArgs].get("company_id").asInstanceOf[StringArgs].get
        val sparkDriver: phSparkDriver = phSparkDriver(job_id)
        import sparkDriver.ss.implicits._

        val cpa = args.asInstanceOf[MapArgs].get("cpa").asInstanceOf[DFArgs].get
                .filter(s"YM like '$ym'")
                .na.fill(value = "0", cols = Array("VALUE", "STANDARD_UNIT"))
        val markets_match = args.asInstanceOf[MapArgs].get("markets_match_file").asInstanceOf[DFArgs].get
        val not_arrival_hosp_file = args.asInstanceOf[MapArgs].get("not_arrival_hosp_file").asInstanceOf[DFArgs].get
        val not_published_hosp_file = args.asInstanceOf[MapArgs].get("not_published_hosp_file").asInstanceOf[DFArgs].get
        val full_hosp_file: DataFrame = args.asInstanceOf[MapArgs].get("full_hosp_file").asInstanceOf[DFArgs].get
        val product_match_file = args.asInstanceOf[MapArgs].get("product_match_file").asInstanceOf[DFArgs].get
        val hosp_ID_file = args.asInstanceOf[MapArgs].get("hosp_ID_file").asInstanceOf[DFArgs].get
        val hosp_ERD = args.asInstanceOf[MapArgs].get("hosp_ERD").asInstanceOf[DFArgs].get
                .withColumnRenamed("PHAIsRepeat", "PHA_IS_REPEAT")
                .withColumnRenamed("PHAHospId", "PHA_HOSP_ID")
                .filter(col("PHA_IS_REPEAT") === "0")
        val prod_DIS = args.asInstanceOf[MapArgs].get("prod_DIS").asInstanceOf[DFArgs].get
        val pha_ERD = args.asInstanceOf[MapArgs].get("pha_ERD").asInstanceOf[DFArgs].get

        def getPanelFile(ym: String, mkt: String): pActionArgs = {
            val fullHosp: DataFrame = fullHosp2ERD(full_hosp_file)
            val full_cpa = fullCPA(cpa, fullHosp)
//            val product_match = trimProductMatch(product_match_file)
            val universe = trimUniverse(hosp_ID_file, mkt)
//            val markets_product_match = product_match.join(markets_match, markets_match("MOLE_NAME") === product_match("通用名"))
            val filted_panel = full_cpa.join(universe, full_cpa("HOSPITAL_ID") === universe("if_panel_hosp_id")).drop("if_panel_hosp_id")
            val panelDF = trimPanel(filted_panel)
            DFArgs(panelDF)
        }

        def fullHosp2ERD(full_hosp_file: DataFrame): DataFrame = {
            val prodCvs = ProductEtcConversion()
            CPAConversion().toERD(MapArgs(Map(
                "cpaDF" -> DFArgs(full_hosp_file.withColumn("COMPANY_ID", lit(company_id)).withColumn("SOURCE", lit("CPA")))
                , "hospDF" -> DFArgs(hosp_ERD)
                , "prodDF" -> DFArgs(prod_DIS)
                , "phaDF" -> DFArgs(pha_ERD)
                , "appendProdFunc" -> SingleArgFuncArgs { args: MapArgs =>
                    prodCvs.toDIS(prodCvs.toERD(args))
                }
            ))).get("cpaERD").asInstanceOf[DFArgs].get.withColumn("SOURCE", lit("CPA"))
        }

        def fullCPA(cpa: DataFrame, fullHosp: DataFrame): DataFrame = {
            val filter_month = ym.takeRight(2).toInt.toString
            val not_arrival_hosp = not_arrival_hosp_file
                    .withColumnRenamed("MONTH", "month")
                    .filter(s"month like '%$filter_month%'")
                    .withColumnRenamed("HOSP_ID", "ID")
                    .select("ID")
            val not_published_hosp = not_published_hosp_file
                    .withColumnRenamed("HOSP_ID", "ID")
            val miss_hosp = not_arrival_hosp.union(not_published_hosp).distinct()
                    .join(pha_ERD, col("ID") === col("CPA"), "left")
                    .select("PHA_ID_NEW")
                    .join(hosp_ERD, col("PHA_ID_NEW") === col("PHA_HOSP_ID"), "left")
                    .selectExpr("_ID as miss_hosp_id")
            val reduced_cpa = cpa.join(miss_hosp, cpa("HOSPITAL_ID") === miss_hosp("miss_hosp_id"), "left").filter("miss_hosp_id is null").drop("miss_hosp_id")
            val full_hosp_id = fullHosp.filter(s"YM == $ym")
            val full_hosp = miss_hosp.join(full_hosp_id, full_hosp_id("HOSPITAL_ID") === miss_hosp("miss_hosp_id")).drop("miss_hosp_id").select(reduced_cpa.columns.head, reduced_cpa.columns.tail: _*)

            reduced_cpa.union(full_hosp)
        }

        def trimUniverse(universe_file: DataFrame, mkt: String): DataFrame = {
            universe_file
                    .withColumn("HOSP_ID", col("PHA_HOSP_ID"))
                    .drop("PHA_HOSP_ID")
                    .join(hosp_ERD.select("PHA_HOSP_ID", "_ID"),col("HOSP_ID") === col("PHA_HOSP_ID"), "left")
                    .filter("IF_PANEL_ALL like '1'")
                    .filter(s"MARKET like '$mkt'")
                    .selectExpr("_ID as if_panel_hosp_id")
        }

        def trimPanel(filted_panel: DataFrame): DataFrame = {
            prod_DIS.select("MARKET", "_id").filter(s"MARKET like '$mkt'")
            filted_panel
                    .join(prod_DIS.selectExpr("MARKET", "_id as market_prod_id").filter(s"MARKET like '$mkt'"), col("PRODUCT_ID") === col("market_prod_id"))
                    .drop("market_prod_id", "MARKET")
                    .groupBy("_id", "PRODUCT_ID", "HOSPITAL_ID", "YM", "COMPANY_ID")
                    .agg(Map("UNITS" -> "sum", "SALES" -> "sum"))
                    .withColumnRenamed("sum(UNITS)", "UNITS")
                    .withColumnRenamed("sum(SALES)", "SALES")
                    .withColumn("_id", commonUDF.generateIdUdf())
        }

        getPanelFile(ym, mkt)
    }
}
