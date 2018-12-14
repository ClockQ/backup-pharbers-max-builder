package com.pharbers.panel.pfizer.actions

import com.pharbers.pactions.actionbase._
import com.pharbers.spark.phSparkDriver

/**
  * Created by jeorch on 18-4-27.
  */
object phPfizerPanelNoSplitAction {
    def apply(args: MapArgs): pActionTrait = new phPfizerPanelNoSplitAction(args)
}

class phPfizerPanelNoSplitAction(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "SplitMarketAction"

    override def perform(args: pActionArgs): pActionArgs = {
        
        val mkt = defaultArgs.asInstanceOf[MapArgs].get("mkt").asInstanceOf[StringArgs].get
        val job_id = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get

        lazy val sparkDriver: phSparkDriver = phSparkDriver(job_id)
        import sparkDriver.ss.implicits._
        
        //通用名市场定义 =>表b0
        val markets_match = args.asInstanceOf[MapArgs].get("markets_match_file").asInstanceOf[DFArgs].get
                .filter(s"MARKET like '$mkt%'")
                .withColumnRenamed("MOLE_NAME", "MOLE_NAME_M")
        //产品标准化 vs IMS_Pfizer_6市场others
        val product_match_file = args.asInstanceOf[MapArgs].get("product_match_file").asInstanceOf[DFArgs].get
                .select("MIN_PRODUCT_UNIT", "MIN_PRODUCT_UNIT_STANDARD", "MOLE_NAME")
                .distinct()
        //表m1
        
        val product_match = mkt match {
            case "DVP" => product_match_file.filter($"MIN_PRODUCT_UNIT_STANDARD" like "得妥%")
            //            case "DVP" => product_match_file.filter("min1 like '得妥%'")
            case _ => product_match_file
        }
        val markets_product_match = product_match.join(markets_match, product_match("MOLE_NAME") === markets_match("MOLE_NAME_M"))
        
        DFArgs(markets_product_match)
    }
    
}