package com.pharbers.nhwa.clean

import com.pharbers.spark.phSparkDriver
import com.pharbers.data.conversion.CPAConversion
import com.pharbers.pactions.actionbase.{MapArgs, pActionArgs, pActionTrait}

/**
  * @description:
  * @author: clock
  * @date: 2019-05-06 11:12
  */
object phNhwaCleanConcretAction {
    def apply(args: MapArgs): pActionTrait = new phNhwaCleanConcretAction(args)
}

case class phNhwaCleanConcretAction(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "phCleanConcretAction"

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.pactions.actionbase._

    override def perform(args: pActionArgs): pActionArgs = {
        val job_id = defaultArgs.getAs[StringArgs]("job_id")
        val company_id = defaultArgs.getAs[StringArgs]("company_id")

        val cpaData = args.getAs[DFArgs]("cpa_data")
        val hospData = args.getAs[DFArgs]("hospital_data")
        val prodData = args.getAs[DFArgs]("product_data")
                .addColumn("PACK_NUMBER").addColumn("PACK_COUNT")
                .withColumn("PACK_NUMBER", when(col("PACK_NUMBER").isNotNull, col("PACK_NUMBER")).otherwise(col("PACK_COUNT")))
        val phaData = args.getAs[DFArgs]("pha_data")
        val phaMatchData = args.getAs[DFArgs]("product_match_data")

        val sparkDriver: phSparkDriver = phSparkDriver(job_id)

        val cpaCvs = CPAConversion()(sparkDriver)
        val cpaERD = cpaCvs.toERD(MapArgs(Map(
            "company_id" -> StringArgs(company_id)
            , "source" -> StringArgs("CPA")
            , "cpaDF" -> DFArgs(cpaData)
            , "hospDF" -> DFArgs(hospData)
            , "prodDF" -> DFArgs(prodData)
            , "phaDF" -> DFArgs(phaData)
            , "prodMatchDF" -> DFArgs(phaMatchData)
            , "matchHospFunc" -> SingleArgFuncArgs(cpaCvs.matchHospFunc)
            , "matchProdFunc" -> SingleArgFuncArgs(cpaCvs.matchProdFunc)
        ))).getAs[DFArgs]("cpaERD")

        DFArgs(cpaERD)
    }
}
