package com.pharbers.unitTest.action

import akka.actor.ActorSystem
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.DataFrame
import com.pharbers.pactions.actionbase._
import org.apache.spark.sql.functions.{expr, lit, udf}
import com.pharbers.pactions.generalactions.readCsvAction
import org.apache.spark.sql.expressions.UserDefinedFunction

object resultCheckAction {
    def apply(args: pActionArgs)(implicit as: ActorSystem): pActionTrait = new resultCheckAction(args)
}

class resultCheckAction(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "resultCheckAction"
    val delimiter: String = 31.toChar.toString

    override def perform(prMap: pActionArgs): pActionArgs = {
        val action = defaultArgs.asInstanceOf[MapArgs].get("checkAction").asInstanceOf[PhActionArgs].get

        val sparkDriver: phSparkDriver = phSparkDriver(action.job_id)
        import sparkDriver.ss.implicits._

        val maxResult = prMap.asInstanceOf[MapArgs].get("executeMaxAction").asInstanceOf[StringArgs].get
        val maxDF = readCsvAction(action.max_path + maxResult, delimiter = delimiter, applicationName = action.job_id)
                .perform(NULLArgs).asInstanceOf[DFArgs].get

        val date: String = maxDF.select("Date").distinct().collect().head.getString(0)
        val mkt: String = maxDF.select("MARKET").distinct().collect().head.getString(0)

        val market = mkt match {
            case "阿洛刻市场" => "Allelock"
            case "米开民市场" => "Mycamine"
            case "普乐可复市场" => "Prograf"
            case "佩尔市场" => "Perdipine"
            case "哈乐市场" => "Harnal"
            case "痛风市场" => "Gout"
            case "卫喜康市场" => "Vesicare"
            case "Grafalon市场" => "Grafalon"
            case "前列腺癌市场" => "前列腺癌"
            case _ => mkt
        }

        // 自定义udf的函数
        val addBooleanCol: UserDefinedFunction = udf{
            (d1: String, d2: String) => (d1.toDouble - d2.toDouble).abs < 1.0E-3
        }

        val offlineDF = prMap.asInstanceOf[MapArgs].get("loadOfflineResult")
                .asInstanceOf[DFArgs].get
                .filter($"YM" === date && $"MKT" === market)
                .withColumnRenamed("CATEGORY", "scope")
                .withColumnRenamed("HOSPITAL_COUNT", "offlineHospNum")
                .withColumnRenamed("PRODUCT_COUNT", "offlineProductNum")
                .withColumnRenamed("f_units(offline)", "offlineUnits")
                .withColumnRenamed("f_sales(offline)", "offlineSales")


//        //保存max结果
//        maxDF.coalesce(1).write
//                .format("csv")
//                .option("header", value = true)
//                .option("delimiter", 31.toChar.toString)
//                .save(s"/mnt/config/result/$market" + "线上max结果")

        //比较全国
        val ckTotalDF: DataFrame = {
            val onlineSales = maxDF.agg(Map("f_sales" -> "sum")).collect().head.getDouble(0).toString
            val onlineUnits = maxDF.agg(Map("f_units" -> "sum")).collect().head.getDouble(0).toString
            val onlineProductNum = maxDF.select("Product").distinct().count().toString
            val onlineHospNum = maxDF.select("Panel_ID").distinct().count().toString
            val ID = "全国"
            val offlineTotalResult = offlineDF.filter(" scope == 'ALL' ")
            val onlineTotalResult = sparkDriver.ss
                    .createDataFrame(
                        Seq(
                            (ID, onlineHospNum, onlineProductNum, onlineSales, onlineUnits)
                        )).toDF("area", "onlineHospNum", "onlineProductNum", "onlineSales", "onlineUnits")
            val totalResult = offlineTotalResult join onlineTotalResult
            val result = totalResult.withColumn("hospBoolean", addBooleanCol(totalResult("offlineHospNum"), totalResult("onlineHospNum")))
                    .withColumn("productBoolean", addBooleanCol(totalResult("offlineProductNum"), totalResult("onlineProductNum")))
                    .withColumn("salesBoolean", addBooleanCol(totalResult("offlineSales"), totalResult("onlineSales")))
                    .withColumn("unitsBoolean", addBooleanCol(totalResult("offlineUnits"), totalResult("onlineUnits")))
            result
        }

//        //比较省份
//        val ckProvinceDF: DataFrame = {
//            val onlineTotalResult = maxDF.select("Province", "Panel_ID", "Product", "f_sales", "f_units")
//                    .groupBy("Province")
//                    .agg(expr("count(distinct Panel_ID)") as "onlineHospNum", expr("count(distinct Product)") as "onlineProductNum", expr("sum(f_units)") as "onlineUnits", expr("sum(f_sales)") as "onlineSales")
//                    .withColumnRenamed("Province", "area")
//            val offlineTotalResult = offlineDF.filter(" scope == 'PROVINCES' ")
//            val totalResult = offlineTotalResult.join(onlineTotalResult, offlineTotalResult("ID") === onlineTotalResult("area"), "full")
//            val nomalTotalResult = totalResult.na.drop()
//                    .withColumn("hospBoolean", addBooleanCol(totalResult("offlineHospNum"), totalResult("onlineHospNum")))
//                    .withColumn("productBoolean", addBooleanCol(totalResult("offlineProductNum"), totalResult("onlineProductNum")))
//                    .withColumn("salesBoolean", addBooleanCol(totalResult("offlineSales"), totalResult("onlineSales")))
//                    .withColumn("unitsBoolean", addBooleanCol(totalResult("offlineUnits"), totalResult("onlineUnits")))
//            val abnomalTotalResult = totalResult.where(totalResult.col("ID").isNull or totalResult.col("area").isNull)
//                    .withColumn("hospBoolean", lit(false))
//                    .withColumn("productBoolean", lit(false))
//                    .withColumn("salesBoolean", lit(false))
//                    .withColumn("unitsBoolean", lit(false))
//            nomalTotalResult.union(abnomalTotalResult)
//        }

        //比较城市
        val ckCityDF: DataFrame = {
            val onlineTotalResult = maxDF.select("City", "Panel_ID", "Product", "f_sales", "f_units")
                    .groupBy("City")
                    .agg(expr("count(distinct Panel_ID)") as "onlineHospNum", expr("count(distinct Product)") as "onlineProductNum", expr("sum(f_sales)") as "onlineSales", expr("sum(f_units)") as "onlineUnits")
                    .withColumnRenamed("City", "area")
            val offlineTotalResult = offlineDF.filter(" scope == 'CITY' ")
            val totalResult = offlineTotalResult.join(onlineTotalResult, offlineTotalResult("ID") === onlineTotalResult("area"), "full")
            val nomalTotalResult = totalResult.na.drop()
                    .withColumn("hospBoolean", addBooleanCol(totalResult("offlineHospNum"), totalResult("onlineHospNum")))
                    .withColumn("productBoolean", addBooleanCol(totalResult("offlineProductNum"), totalResult("onlineProductNum")))
                    .withColumn("salesBoolean", addBooleanCol(totalResult("offlineSales"), totalResult("onlineSales")))
                    .withColumn("unitsBoolean", addBooleanCol(totalResult("offlineUnits"), totalResult("onlineUnits")))
            val abnomalTotalResult = totalResult.where(totalResult.col("ID").isNull or totalResult.col("area").isNull)
                    .withColumn("hospBoolean", lit(false))
                    .withColumn("productBoolean", lit(false))
                    .withColumn("salesBoolean", lit(false))
                    .withColumn("unitsBoolean", lit(false))
            nomalTotalResult.union(abnomalTotalResult)
        }

        //比较医院
        val ckHospDF: DataFrame = {
            val onlineTotalResult = maxDF.select("Panel_ID", "Product", "f_sales", "f_units")
                    .withColumnRenamed("Panel_ID", "area")
                    .groupBy("area")
                    .agg(expr("count(distinct area)") as "onlineHospNum", expr("count(distinct Product)") as "onlineProductNum", expr("sum(f_sales)") as "onlineSales", expr("sum(f_units)") as "onlineUnits")
            val offlineTotalResult = offlineDF.filter(" scope == 'HOSP' ")
            val totalResult = offlineTotalResult.join(onlineTotalResult, offlineTotalResult("ID") === onlineTotalResult("area"), "full").persist()

            val nomalTotalResult = totalResult.na.drop()
                    .withColumn("hospBoolean", addBooleanCol(totalResult("offlineHospNum"), totalResult("onlineHospNum")))
                    .withColumn("productBoolean", addBooleanCol(totalResult("offlineProductNum"), totalResult("onlineProductNum")))
                    .withColumn("salesBoolean", addBooleanCol(totalResult("offlineSales"), totalResult("onlineSales")))
                    .withColumn("unitsBoolean", addBooleanCol(totalResult("offlineUnits"), totalResult("onlineUnits")))
            val abnomalTotalResult = totalResult.where(totalResult.col("ID").isNull or totalResult.col("area").isNull)
                    .withColumn("hospBoolean", lit(false))
                    .withColumn("productBoolean", lit(false))
                    .withColumn("salesBoolean", lit(false))
                    .withColumn("unitsBoolean", lit(false))
            nomalTotalResult.union(abnomalTotalResult)
        }

        DFArgs(ckTotalDF.union(
            (ckCityDF union ckHospDF)
                    .na.fill(value = date, cols = Array("Date"))
                    .na.fill(value = market, cols = Array("market"))
                    .where("hospBoolean = false or productBoolean = false or salesBoolean = false or unitsBoolean = false"))
        )
    }
}