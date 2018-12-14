package com.pharbers.panel.astellas

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase._

object phAstellasPanelConcretJob {
    def apply(args: MapArgs): pActionTrait = new phAstellasPanelConcretJob(args)
}

class phAstellasPanelConcretJob(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "panel"
    
    lazy val sparkDriver: phSparkDriver = phSparkDriver()
    import sparkDriver.ss.implicits._
    
    override def perform(args: pActionArgs): pActionArgs = {
        
        val ym = defaultArgs.asInstanceOf[MapArgs].get("ym").asInstanceOf[StringArgs].get
        val mkt = defaultArgs.asInstanceOf[MapArgs].get("mkt").asInstanceOf[StringArgs].get
        val mkt_en = defaultArgs.asInstanceOf[MapArgs].get("mkt_en").asInstanceOf[StringArgs].get
        
        val cpa = args.asInstanceOf[MapArgs].get("cpa").asInstanceOf[DFArgs].get
                .filter(col("YM") === ym)
                .withColumn("PRODUCT_NAME", when(col("PRODUCT_NAME_NOTE").isNotNull, col("PRODUCT_NAME_NOTE"))
                        .otherwise(col("PRODUCT_NAME")))
                .withColumn("PRODUCT_NAME", when(col("PRODUCT_NAME").isNull, col("MOLE_NAME"))
                        .otherwise(col("PRODUCT_NAME")))
                .withColumn("HOSP_ID", when(col("HOSP_ID") === "230231", "230233")
                        .otherwise(col("HOSP_ID")))
                .withColumn("HOSP_ID", when(col("HOSP_ID") === "110561", "110563")
                        .otherwise(col("HOSP_ID")))
                .withColumn("VALUE", when(col("STANDARD_UNIT") === "0", "0")
                        .otherwise(col("VALUE")))
                .withColumn("STANDARD_UNIT", when(col("VALUE") === "0", "0")
                        .otherwise(col("STANDARD_UNIT")))
                .withColumn("PRODUCT_NAME", trim(col("PRODUCT_NAME")))
                .withColumn("DOSAGE", trim(col("DOSAGE")))
                .withColumn("PACK_DES", trim(col("PACK_DES")))
                .withColumn("PACK_NUMBER", trim(col("PACK_NUMBER")))
                .withColumn("CORP_NAME", trim(col("CORP_NAME")))
                .withColumn("min1", concat(col("PRODUCT_NAME"), col("DOSAGE"), col("PACK_DES"), col("PACK_NUMBER"), col("CORP_NAME")))
        
        val gycx = args.asInstanceOf[MapArgs].get("gycx").asInstanceOf[DFArgs].get
                .filter(col("YM") === ym)
                .withColumn("PRODUCT_NAME", when(col("PRODUCT_NAME").isNull, col("MOLE_NAME"))
                        .otherwise(col("PRODUCT_NAME")))
                .withColumn("VALUE", when(col("STANDARD_UNIT") === "0", "0")
                        .otherwise(col("VALUE")))
                .withColumn("STANDARD_UNIT", when(col("VALUE") === "0", "0")
                        .otherwise(col("STANDARD_UNIT")))
                .withColumn("PRODUCT_NAME", when(col("PRODUCT_NAME").isNull, col("MOLE_NAME"))
                        .otherwise(col("PRODUCT_NAME")))
                .withColumn("PRODUCT_NAME", trim(col("PRODUCT_NAME")))
                .withColumn("DOSAGE", trim(col("DOSAGE")))
                .withColumn("PACK_DES", trim(col("PACK_DES")))
                .withColumn("PACK_NUMBER", trim(col("PACK_NUMBER")))
                .withColumn("CORP_NAME", trim(col("CORP_NAME")))
                .withColumn("min1", concat(col("PRODUCT_NAME"), col("DOSAGE"), col("PACK_DES"), col("PACK_NUMBER"), col("CORP_NAME")))
        
        val product_match_file = args.asInstanceOf[MapArgs].get("product_match_file").asInstanceOf[DFArgs].get
                .withColumn("STANDARD_PACK_NUMBER",
                    when(col("STANDARD_PACK_NUMBER").isNull, col("PACK_COUNT"))
                            .otherwise(col("STANDARD_PACK_NUMBER")))
                .withColumn("STANDARD_MOLE_NAME",
                    when(col("STANDARD_MOLE_NAME") === "抗人胸腺细胞兔免疫球蛋白", "抗人胸腺细胞免疫球蛋白")
                            .otherwise(col("STANDARD_MOLE_NAME")))
                .withColumn("STANDARD_MOLE_NAME",
                    when(col("STANDARD_PRODUCT_NAME") === "米芙", "麦考芬酸钠")
                            .otherwise(col("STANDARD_MOLE_NAME")))
                .withColumn("STANDARD_PRODUCT_NAME",
                    when(col("STANDARD_PRODUCT_NAME") === "哈乐" && col("STANDARD_DOSAGE") === "片剂" && col("STANDARD_PACK_NUMBER") === "14", "新哈乐")
                            .otherwise(col("STANDARD_PRODUCT_NAME")))
                .withColumn("STANDARD_PRODUCT_NAME",
                    when(col("STANDARD_PRODUCT_NAME") === "新哈乐" && col("STANDARD_DOSAGE") === "片剂" && col("STANDARD_PACK_NUMBER") === "10", "哈乐")
                            .otherwise(col("STANDARD_PRODUCT_NAME")))
        
        
        val markets_match = args.asInstanceOf[MapArgs].get("markets_match_file").asInstanceOf[DFArgs].get
        val hospital = args.asInstanceOf[MapArgs].get("hospital_file").asInstanceOf[DFArgs].get
                .withColumn("STANDARD_ID", when(col("ACN_ID").isNotNull, col("ACN_ID"))
                        .otherwise(when(col("CPA_ID").isNotNull, col("CPA_ID"))
                                .otherwise(col("GYCX_ID"))))
                .withColumn("STANDARD_HOSP_NAME", when(col("ACN_HOSP_NAME").isNotNull, col("ACN_HOSP_NAME"))
                        .otherwise(when(col("CPA_HOSP_NAME").isNotNull, col("CPA_HOSP_NAME"))
                                .otherwise(col("GYCX_HOSP_NAME"))))
                .withColumn("STANDARD_HOSP_LEVEL", when(col("ACN_HOSP_LEVEL").isNotNull, col("ACN_HOSP_LEVEL"))
                        .otherwise(when(col("CPA_HOSP_LEVEL").isNotNull, col("CPA_HOSP_LEVEL"))
                                .otherwise(col("GYCX_HOSP_LEVEL"))))
                .withColumn("SOURCE", when(col("STANDARD_ID") === col("ACN_ID"), "ACN")
                        .otherwise(when(col("STANDARD_ID") === col("CPA_ID"), "CPA")
                                .otherwise("GYC")))
        val hosp_ID = args.asInstanceOf[MapArgs].get("hosp_ID_file").asInstanceOf[DFArgs].get
        
        // 自定义的udf函数，去除分隔符
        val replaceDelimiter = udf((str: String) => {
            str.replaceAll("\\|", "")
        })
        
        val replaceMarket = udf((str: String) => {
            str match {
                case `mkt_en` => mkt
                case _ => str
            }
        })
        // 去除GYC中的重复医院
        val delete_double_gycx: DataFrame = {
            val hospital_cpa = hospital.filter("CPA_DIS is null")
                    .filter("CPA_ID is not null")
            val hospital_gyc = hospital.filter("GYC_DIS is null")
                    .filter("GYCX_ID is not null")
            val standard_cpa_code = cpa.join(hospital_cpa, cpa("HOSP_ID") === hospital_cpa("CPA_ID"))
                    .withColumn("JR_CODE", concat(col("YM"), col("STANDARD_ID"))).select("JR_CODE")
            val standard_gyc_code = gycx.join(hospital_gyc, gycx("HOSP_ID") === hospital_gyc("GYCX_ID"))
                    .withColumn("JR_CODE", concat(col("YM"), col("STANDARD_ID")))
            val double = standard_cpa_code.intersect(standard_gyc_code.select("JR_CODE")).withColumnRenamed("JR_CODE", "ID")
            
            standard_gyc_code.join(double, standard_gyc_code("JR_CODE") === double("ID"), "left")
                    .filter("ID is null").drop("ID", "JR_CODE")
                    .withColumnRenamed("MOLE_NAME", "GYC_MOLE_NAME")
        }
        
        // 合并cpa和gycx的源数据
        val total = {
            val filtered_cpa = cpa.select("HOSP_ID", "YM", "MOLE_NAME", "min1", "MARKET", "VALUE", "STANDARD_UNIT")
            
            val have_market_gycx = delete_double_gycx.join(markets_match, delete_double_gycx("GYC_MOLE_NAME") === markets_match("MOLE_NAME"))
                    .withColumn("MARKET", replaceMarket(col("MARKET")))
                    .select(filtered_cpa.columns.head, filtered_cpa.columns.tail: _*).distinct()
            
            filtered_cpa.union(have_market_gycx)
                    .withColumn("VALUE", 'VALUE.cast(DoubleType))
                    .withColumnRenamed("VALUE", "Sales")
                    .withColumn("STANDARD_UNIT", 'STANDARD_UNIT.cast(DoubleType))
                    .withColumnRenamed("STANDARD_UNIT", "Units")
                    .withColumn("HOSP_ID", 'HOSP_ID.cast(LongType))
                    .filter(col("MARKET") === mkt)
        }
        
        val product_match = product_match_file
                .withColumn("MIN_PRODUCT_UNIT", replaceDelimiter(col("MIN_PRODUCT_UNIT")))
                .select("MIN_PRODUCT_UNIT", "MIN_PRODUCT_UNIT_STANDARD", "STANDARD_MOLE_NAME", "STANDARD_DOSAGE", "STANDARD_PRODUCT_NAME")
                .distinct()
        
        // 清洗市场
        val modifiedMarketTotal = {
            total.join(product_match, total("min1") === product_match("MIN_PRODUCT_UNIT"))
                    .withColumn("NEW_MARKET",
                        when(col("STANDARD_MOLE_NAME") === "他克莫司" && col("STANDARD_DOSAGE") === "软膏剂", "普特彼市场")
                                .when(col("STANDARD_MOLE_NAME") === "他克莫司" && col("STANDARD_DOSAGE") =!= "软膏剂", "普乐可复市场")
                                .otherwise(col("MARKET"))
                    ).drop("MARKET").withColumnRenamed("NEW_MARKET", "MARKET")
        }
        
        // 删除一些数据
        val deletedTotal = {
            modifiedMarketTotal.filter(!(col("MARKET") === "佩尔市场" && (col("STANDARD_DOSAGE") =!= "粉针剂" && col("STANDARD_DOSAGE") =!= "注射剂")))
                    .filter(!(col("MARKET") === "阿洛刻市场" && col("STANDARD_DOSAGE") === "粉针剂"))
                    .filter(!(col("MARKET") === "阿洛刻市场" && col("STANDARD_DOSAGE") === "注射剂"))
                    .filter(!(col("MARKET") === "阿洛刻市场" && col("STANDARD_DOSAGE") === "滴眼剂"))
                    .filter(!(col("MARKET") === "阿洛刻市场" && col("STANDARD_DOSAGE") === "喷雾剂"))
                    .filter(!(col("MARKET") === "阿洛刻市场" && col("STANDARD_DOSAGE") === "汽雾剂"))
                    .filter(!(col("MARKET") === "米开民市场" && col("STANDARD_DOSAGE") === "颗粒剂"))
                    .filter(!(col("MARKET") === "米开民市场" && col("STANDARD_DOSAGE") === "胶囊剂"))
                    .filter(!(col("MARKET") === "米开民市场" && col("STANDARD_DOSAGE") === "滴眼剂"))
                    .filter(!(col("MARKET") === "米开民市场" && col("STANDARD_DOSAGE") === "口服溶剂"))
                    .filter(!(col("MARKET") === "米开民市场" && col("STANDARD_DOSAGE") === "片剂"))
                    .filter(!(col("MARKET") === "普乐可复市场" && col("STANDARD_DOSAGE") === "滴眼剂"))
                    .filter(!(col("STANDARD_PRODUCT_NAME") === "保法止"))
                    .filter(!(col("MIN_PRODUCT_UNIT_STANDARD") === "先立晓|片剂|1MG|10|浙江仙琚制药股份有限公司"))
                    .filter(!(col("STANDARD_MOLE_NAME") === "倍他司汀"))
                    .filter(!(col("STANDARD_MOLE_NAME") === "阿魏酰γ-丁二胺/植物生长素"))
                    .filter(!(col("STANDARD_MOLE_NAME") === "丙磺舒"))
                    .filter(!(col("STANDARD_MOLE_NAME") === "复方别嘌醇"))
        }
        
        // group 后 求和
        val groupedTotal = {
            deletedTotal.withColumnRenamed("HOSP_ID", "HOSP_ID_TOTAL")
                    .withColumn("Sales", 'Sales.cast(DoubleType))
                    .withColumn("Units", 'Units.cast(DoubleType))
                    .groupBy("HOSP_ID_TOTAL", "YM", "MIN_PRODUCT_UNIT_STANDARD", "MARKET")
                    .agg(Map("Units" -> "sum", "Sales" -> "sum"))
                    .withColumnRenamed("sum(Units)", "Units")
                    .withColumnRenamed("sum(Sales)", "Sales")
        }
        
        //
        // 处理IF_PANEL_FILE,只保留样本医院
        val universeCode = {
            hosp_ID.withColumnRenamed("HOSP_ID", "PANLE_ID")
                    .withColumnRenamed("PHA_HOSP_NAME", "HOSP_NAME")
                    .withColumnRenamed("PHA_HOSP_ID", "HOSP_ID")
                    .select("PANLE_ID", "HOSP_ID", "HOSP_NAME")
                    .filter(col("PANLE_ID") =!= "" && col("PANLE_ID") =!= " ")
                    .withColumn("PANLE_ID", 'PANLE_ID.cast(LongType))
        }
        
        // 根据universeCode，只保留样本医院panel
        val panel = {
            groupedTotal.join(universeCode, groupedTotal("HOSP_ID_TOTAL") === universeCode("PANLE_ID"))
                    .filter(col("MARKET") === mkt)
                    .withColumn("ID", col("PANLE_ID").cast(LongType))
                    .withColumn("Hosp_name", col("HOSP_NAME"))
                    .withColumn("Date", col("YM"))
                    .withColumn("Prod_Name", col("MIN_PRODUCT_UNIT_STANDARD"))
                    .withColumn("Prod_CNAME", col("MIN_PRODUCT_UNIT_STANDARD"))
                    .withColumn("Strength", col("MIN_PRODUCT_UNIT_STANDARD"))
                    .withColumn("DOI", col("MARKET"))
                    .withColumn("DOIE", col("MARKET"))
                    .select("ID", "Hosp_name", "Date",
                        "Prod_Name", "Prod_CNAME", "HOSP_ID", "Strength",
                        "DOI", "DOIE", "Units", "Sales")
        }
        
        DFArgs(panel)
    }
    
}