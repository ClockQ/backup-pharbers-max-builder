package com.pharbers.nhwa

import java.util.UUID

import com.pharbers.spark.phSparkDriver
import com.pharbers.nhwa.calc.phNhwaMaxJob
import akka.actor.{ActorSelection, ActorSystem}
import com.pharbers.channel.consumer.commonXmppConsumer
import com.pharbers.channel.detail.channelEntity
import com.pharbers.channel.driver.xmpp.xmppFactor
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object testNhwaCalc extends App {
//    val job_id: String = "job_id"
//    val panel_name: String = "b436b2f3-d0ef-4a16-bdfc-b175745ef292"
//    println(s"panel_name = $panel_name")
//    val max_name: String = UUID.randomUUID().toString
//    println(s"max_name = $max_name")
//    val max_search_name: String = UUID.randomUUID().toString
//    println(s"max_search_name = $max_search_name")
//
//    val map: Map[String, String] = Map(
//        "panel_path" -> "hdfs:///workData/Panel/",
//        "panel_name" -> panel_name,
//        "max_path" -> "hdfs:///workData/Panel/",
//        "max_name" -> max_name,
//        "max_search_name" -> max_search_name,
//        "ym" -> "201804",
//        "mkt" -> "麻醉市场",
//        "job_id" -> job_id,
//        "user_id" -> "user_id",
//        "company_id" -> "company_id",
//        "prod_lst" -> "恩华",
//        "p_current" -> "1",
//        "p_total" -> "1",
//        "universe_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_universe_麻醉市场_20180705.csv"
//    )
//
//    implicit val system: ActorSystem = ActorSystem("maxActor")
//    implicit val xmppconfig: XmppConfigType = Map(
//        "xmpp_host" -> "192.168.100.172",
//        "xmpp_port" -> "5222",
//        "xmpp_user" -> "driver",
//        "xmpp_pwd" -> "driver",
//        "xmpp_listens" -> "lu@localhost",
//        "xmpp_pool_num" -> "1"
//    )
//    val acter_location: String = xmppFactor.startLocalClient(new commonXmppConsumer)
//    val lactor: ActorSelection = system.actorSelection(acter_location)
////    val lactor: ActorSelection = system.actorSelection(xmppFactor.getNullActor)
//    val send: channelEntity => Unit = {
//        obj => lactor ! ("lu@localhost#alfred@localhost", obj)
//    }
//
//    val result = phNhwaMaxJob(map)(send).perform()
//            .asInstanceOf[MapArgs].get("result")
//            .asInstanceOf[StringArgs].get
//    println(result)
//
////    phSparkDriver(job_id).stopCurrConn

    implicit val sd: phSparkDriver = phSparkDriver("abc")

    import sd.ss.implicits._
    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._

    val panelDF = Parquet2DF("/test/qi/qi/panel5").drop("_id")
    val universeDF = Parquet2DF("/repository/universe_hosp" + "/" + "5ca069bceeefcc012918ec72" + "/" + "mz").drop("_id")
//            .withColumnRenamed("PHA_HOSP_ID", "PHA_ID")
            .withColumnRenamed("IF_PANEL_ALL", "IS_PANEL_HOSP")
            .withColumnRenamed("IF_PANEL_TO_USE", "NEED_MAX_HOSP")
            .withColumnRenamed("FACTOR", "Factor")
//            .withColumnRenamed("PROVINCE", "Province")
            .withColumnRenamed("WEST_MEDICINE_INCOME", "westMedicineIncome")
//            .withColumnRenamed("PREFECTURE", "Prefecture")
//            .selectExpr("PHA_ID", "Factor", "IS_PANEL_HOSP", "NEED_MAX_HOSP", "SEGMENT", "Province", "Prefecture", "westMedicineIncome")

    val panelSummed = panelDF.groupBy("YM", "HOSPITAL_ID", "PRODUCT_ID")
            .agg(Map("UNITS" -> "sum", "SALES" -> "sum"))
            .withColumnRenamed("YM", "P_YM")
            .withColumnRenamed("HOSPITAL_ID", "P_HOSPITAL_ID")
            .withColumnRenamed("PRODUCT_ID", "P_PRODUCT_ID")
            .withColumnRenamed("sum(UNITS)", "sumUnits")
            .withColumnRenamed("sum(SALES)", "sumSales")

    val joinDataWithEmptyValue = panelDF.select("YM", "PRODUCT_ID").distinct() crossJoin universeDF

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
                .agg(Map("sumSales" -> "sum", "sumUnits" -> "sum", "westMedicineIncome" -> "sum"))
                .withColumnRenamed("SEGMENT", "s_SEGMENT")
                .withColumnRenamed("PRODUCT_ID", "s_PRODUCT_ID")
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
                            && joinData("PRODUCT_ID") === segmentDF("s_PRODUCT_ID")
                            && joinData("YM") === segmentDF("s_YM"))
                .drop("s_SEGMENT", "s_PRODUCT_ID", "s_YM")
                .withColumn("Factor", 'Factor.cast(DoubleType))
                .filter("Factor > 0")
                .withColumn("f_sales",
                    when($"IS_PANEL_HOSP" === 1, $"sumSales").otherwise(
                        when($"avg_Sales" <= 0.0 or $"avg_Units" <= 0.0, 0.0)
                                .otherwise($"Factor" * $"avg_Sales" * $"westMedicineIncome")
                    ).cast(DoubleType))
                .withColumn("f_units",
                    when($"IS_PANEL_HOSP" === 1, $"sumUnits").otherwise(
                        when($"avg_Sales" <= 0.0 or $"avg_Units" <= 0.0, 0.0)
                                .otherwise($"Factor" * $"avg_Units" * $"westMedicineIncome")
                    ).cast(DoubleType))
                .drop("s_sumSales", "s_sumUnits", "s_westMedicineIncome")
                .withColumn("flag",
                    when($"IS_PANEL_HOSP" === 1, 1).otherwise(
                        when($"f_units" === 0 and $"f_sales" === 0, 0).otherwise(1)
                    ))
                .filter($"flag" === 1 && $"IS_PANEL_HOSP" === 0)
                .withColumn("Date", 'YM.cast(IntegerType))
                .select("Date", "HOSPITAL_ID", "PRODUCT_ID", "f_units", "f_sales")
    }

    val backfillDF = {
        panelDF.join(universeDF, panelDF("HOSPITAL_ID") === universeDF("HOSPITAL_ID"))
                .drop(panelDF("HOSPITAL_ID"))
                .withColumn("Date", 'YM.cast(IntegerType))
//                .withColumnRenamed("Prefecture", "City")
//                .withColumnRenamed("PHA_ID", "Panel_ID")
//                .withColumnRenamed("min1", "Product")
                .withColumnRenamed("Sales", "f_sales")
                .withColumnRenamed("Units", "f_units")
                .select("Date", "HOSPITAL_ID", "PRODUCT_ID", "f_units", "f_sales")
//                .selectExpr("Date", "Province", "City", "Panel_ID", "Product", "Factor", "f_sales", "f_units", "MARKET")
    }

    val maxDF = backfillDF.unionByName(enlargedDF)
    maxDF.show(false)
    println(maxDF.count())
    maxDF.save2Parquet("/test/qi/qi/max5")
}
//+------+------------------------+----+----+------+-------+-----------+-------------+-------------+-------+------------------------------+--------+----------+--------------+------------------+------------------------+----+-------------+------------+--------+--------+--------------------+--------------------+-------------------+--------------------+----+------+
//|YM    |PRODUCT_ID              |公司  |YEAR|MARKET|SEGMENT|Factor     |IS_PANEL_HOSP|NEED_MAX_HOSP|HOSP_ID|PHA_HOSP_NAME                 |PROVINCE|PREFECTURE|CITY_TIER_2010|westMedicineIncome|HOSPITAL_ID             |P_YM|P_HOSPITAL_ID|P_PRODUCT_ID|sumSales|sumUnits|avg_Sales           |avg_Units           |f_sales            |f_units             |flag|Date  |
//+------+------------------------+----+----+------+-------+-----------+-------------+-------------+-------+------------------------------+--------+----------+--------------+------------------+------------------------+----+-------------+------------+--------+--------+--------------------+--------------------+-------------------+--------------------+----+------+
//+------------------------+------+------------------------+------------------------+-------+--------+----+----+------+-------+------------------+-------------+-------------+-------+-------------+--------+----------+--------------+------------------+------------------------+------+
//|COMPANY_ID              |YM    |HOSPITAL_ID             |PRODUCT_ID              |f_units|f_sales |公司  |YEAR|MARKET|SEGMENT|Factor            |IS_PANEL_HOSP|NEED_MAX_HOSP|HOSP_ID|PHA_HOSP_NAME|PROVINCE|PREFECTURE|CITY_TIER_2010|westMedicineIncome|HOSPITAL_ID             |Date  |
//+------------------------+------+------------------------+------------------------+-------+--------+----+----+------+-------+------------------+-------------+-------------+-------+-------------+--------+----------+--------------+------------------+------------------------+------+