package com.pharbers.panel.nhwa

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}

object phNhwaPanelConcretJob {
    def apply(args: MapArgs): pActionTrait = new phNhwaPanelConcretJob(args)
}

class phNhwaPanelConcretJob(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "panel"

    override def perform(args: pActionArgs): pActionArgs = {
        
        val ym = defaultArgs.asInstanceOf[MapArgs].get("ym").asInstanceOf[StringArgs].get
        val mkt = defaultArgs.asInstanceOf[MapArgs].get("mkt").asInstanceOf[StringArgs].get
        val job_id = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get
        lazy val sparkDriver: phSparkDriver = phSparkDriver(job_id)
        import sparkDriver.ss.implicits._

        val cpa = args.asInstanceOf[MapArgs].get("cpa").asInstanceOf[DFArgs].get
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
        val markets_match = args.asInstanceOf[MapArgs].get("markets_match_file").asInstanceOf[DFArgs].get
        val not_arrival_hosp_file = args.asInstanceOf[MapArgs].get("not_arrival_hosp_file").asInstanceOf[DFArgs].get
        val not_published_hosp_file = args.asInstanceOf[MapArgs].get("not_published_hosp_file").asInstanceOf[DFArgs].get
        val full_hosp_file: DataFrame = args.asInstanceOf[MapArgs].get("full_hosp_file").asInstanceOf[DFArgs].get
                .withColumn("MONTH", when(col("MONTH").>=(10), col("MONTH"))
                        .otherwise(concat(col("MONTH").*(0).cast("int"), col("MONTH"))))
                .withColumn("YM", concat(col("YEAR"), col("MONTH")))
                .withColumn("PRODUCT_NAME", trim(col("PRODUCT_NAME")))
                .withColumn("DOSAGE", trim(col("DOSAGE")))
                .withColumn("PACK_DES", trim(col("PACK_DES")))
                .withColumn("PACK_NUMBER", trim(col("PACK_NUMBER")))
                .withColumn("CORP_NAME", trim(col("CORP_NAME")))
                .withColumn("min1", concat(col("PRODUCT_NAME"), col("DOSAGE"), col("PACK_DES"), col("PACK_NUMBER"), col("CORP_NAME")))
        val product_match_file = args.asInstanceOf[MapArgs].get("product_match_file").asInstanceOf[DFArgs].get
        val hosp_ID_file = args.asInstanceOf[MapArgs].get("hosp_ID_file").asInstanceOf[DFArgs].get
        
        def getPanelFile(ym: String, mkt: String): pActionArgs = {
            val full_cpa = fullCPA(cpa, ym).withColumnRenamed("HOSP_ID", "HOSP_ID_cpa")
                    .withColumn("HOSP_ID_cpa", 'HOSP_ID_cpa.cast(IntegerType))
            val product_match = trimProductMatch(product_match_file)
            val universe = trimUniverse(hosp_ID_file, mkt).withColumn("ID", 'ID.cast(IntegerType))
            val markets_product_match = product_match.join(markets_match, markets_match("MOLE_NAME") === product_match("通用名"))
            val filted_panel = full_cpa.join(universe, full_cpa("HOSP_ID_cpa") === universe("ID"))
            val panelDF = trimPanel(filted_panel, markets_product_match)
            DFArgs(panelDF)
        }
        
        def fullCPA(cpa: DataFrame, ym: String): DataFrame = {
            val filter_month = ym.takeRight(2).toInt.toString
            val filter_month_full_hosp = ym.takeRight(2).toString
            val primal_cpa = cpa.filter(s"YM like '$ym'")
            val not_arrival_hosp = not_arrival_hosp_file
                    .withColumnRenamed("MONTH", "month")
                    .filter(s"month like '%$filter_month%'")
                    .withColumnRenamed("HOSP_ID", "ID")
                    .select("ID")
            val not_published_hosp = not_published_hosp_file
                    .withColumnRenamed("HOSP_ID", "ID")
            val miss_hosp = not_arrival_hosp.union(not_published_hosp).distinct()
                    .withColumn("ID", 'ID.cast(IntegerType))
            val reduced_cpa = primal_cpa.join(miss_hosp, primal_cpa("HOSP_ID") === miss_hosp("ID"), "left").filter("ID is null").drop("ID")
            val full_hosp_id = full_hosp_file.filter(s"MONTH == $filter_month_full_hosp")
                    .withColumn("HOSP_ID", 'HOSP_ID.cast(IntegerType))
            val full_hosp = miss_hosp.join(full_hosp_id, full_hosp_id("HOSP_ID") === miss_hosp("ID")).drop("ID").select(reduced_cpa.columns.head, reduced_cpa.columns.tail: _*)
            
            import sparkDriver.ss.implicits._
            reduced_cpa.union(full_hosp).withColumn("HOSP_ID", 'HOSP_ID.cast(LongType))
        }
        
        def trimProductMatch(product_match_file: DataFrame): DataFrame = {
            product_match_file
                    .withColumnRenamed("MOLE_NAME", "NAME")
                    .withColumnRenamed("DOSAGE", "APP2_COD")
                    .withColumnRenamed("PACK_COUNT", "PACK_NUMBER")
                    .withColumnRenamed("STANDARD_PRODUCT_NAME", "s_PRODUCT_NAME")
                    .withColumnRenamed("STANDARD_PACK_DES", "s_PACK_DES")
                    .withColumnRenamed("STANDARD_DOSAGE", "s_APP2_COD")
                    .withColumnRenamed("STANDARD_CORP_NAME", "s_CORP_NAME")
                    .withColumn("min2",
                        when(col("MIN_PRODUCT_UNIT_STANDARD") =!= "", col("MIN_PRODUCT_UNIT_STANDARD"))
                                .otherwise(col("s_PRODUCT_NAME") + col("s_APP2_COD") + col("s_PACK_DES") + col("PACK_NUMBER") + col("PACK_NUMBER"))
                    )
                    .selectExpr("concat(PRODUCT_NAME,APP2_COD,PACK_DES,PACK_NUMBER,CORP_NAME) as min1", "min2", "NAME")
                    .withColumnRenamed("min2", "min1_标准")
                    .withColumnRenamed("NAME", "通用名")
                    .distinct()
        }
        
        def trimUniverse(universe_file: DataFrame, mkt: String): DataFrame = {
            import sparkDriver.ss.implicits._
            universe_file.withColumnRenamed("HOSP_ID", "ID")
                    .withColumnRenamed("PHA_HOSP_NAME", "HOSP_NAME")
                    .withColumnRenamed("PHA_HOSP_ID", "HOSP_ID")
                    .withColumnRenamed("MARKET", "DOI")
                    .withColumnRenamed("IF_PANEL_ALL", "SAMPLE")
                    .filter("SAMPLE like '1'")
                    .selectExpr("ID", "HOSP_NAME", "HOSP_ID", "DOI", "DOI as DOIE")
                    .filter(s"DOI like '$mkt'")
                    .withColumn("ID", 'ID.cast(LongType))
        }
        
        def trimPanel(filted_panel: DataFrame, markets_product_match: DataFrame): DataFrame = {
            import sparkDriver.ss.implicits._
            val temp = filted_panel.join(markets_product_match, filted_panel("min1") === markets_product_match("min1"))
                    .withColumn("ID", 'ID.cast(LongType))
                    .withColumnRenamed("HOSP_NAME", "Hosp_name")
                    .withColumnRenamed("YM", "Date")
                    .withColumnRenamed("min1_标准", "Prod_Name")
                    .withColumn("VALUE", 'VALUE.cast(DoubleType))
                    .withColumnRenamed("VALUE", "Sales")
                    .withColumn("STANDARD_UNIT", 'STANDARD_UNIT.cast(DoubleType))
                    .withColumnRenamed("STANDARD_UNIT", "Units")
                    .selectExpr("ID", "Hosp_name", "Date",
                        "Prod_Name", "Prod_Name as Prod_CNAME", "HOSP_ID", "Prod_Name as Strength",
                        "DOI", "DOIE", "Units", "Sales")
            
            temp.groupBy("ID", "Hosp_name", "Date", "Prod_Name", "Prod_CNAME", "HOSP_ID", "Strength", "DOI", "DOIE")
                    .agg(Map("Units" -> "sum", "Sales" -> "sum"))
                    .withColumnRenamed("sum(Units)", "Units")
                    .withColumnRenamed("sum(Sales)", "Sales")
        }
        
        getPanelFile(ym, mkt)
    }
    
}