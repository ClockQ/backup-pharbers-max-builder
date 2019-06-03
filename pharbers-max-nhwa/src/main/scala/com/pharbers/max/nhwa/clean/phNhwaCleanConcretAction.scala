package com.pharbers.max.nhwa.clean

import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase._
import com.pharbers.data.conversion.CPAConversion

object phNhwaCleanConcretAction {
    def apply(args: MapArgs)(implicit sparkDriver: phSparkDriver): pActionTrait = new phNhwaCleanConcretAction(args)
}

case class phNhwaCleanConcretAction(override val defaultArgs: pActionArgs)(implicit sparkDriver: phSparkDriver) extends pActionTrait {
    override val name: String = "phNhwaCleanConcretAction"

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._

    override def perform(args: pActionArgs): pActionArgs = {
        val company_id = defaultArgs.getAs[StringArgs]("company_id")

        val cpaData = args.getAs[DFArgs]("cpa_data")
        val hospData = args.getAs[DFArgs]("hospital_data")
        val prodData = args.getAs[DFArgs]("product_data")
                .addColumn("PACK_NUMBER").addColumn("PACK_COUNT")
                .withColumn("PACK_NUMBER", when(col("PACK_NUMBER").isNotNull, col("PACK_NUMBER")).otherwise(col("PACK_COUNT")))
        val phaData = args.getAs[DFArgs]("pha_data")
        val productMatchData = args.getAs[DFArgs]("product_match_data")

        val cpaCvs = CPAConversion()(sparkDriver)
        val cpaERD = cpaCvs.toERD(MapArgs(Map(
            "company_id" -> StringArgs(company_id)
            , "source" -> StringArgs("CPA")
            , "cpaDF" -> DFArgs(cpaData)
            , "hospDF" -> DFArgs(hospData)
            , "prodDF" -> DFArgs(prodData)
            , "phaDF" -> DFArgs(phaData)
            , "prodMatchDF" -> DFArgs(productMatchData)
            , "matchHospFunc" -> SingleArgFuncArgs(cpaCvs.matchHospFunc)
            , "matchProdFunc" -> SingleArgFuncArgs(cpaCvs.matchProdFunc)
        ))).getAs[DFArgs]("cpaERD")

        DFArgs(cpaERD)
    }
}
