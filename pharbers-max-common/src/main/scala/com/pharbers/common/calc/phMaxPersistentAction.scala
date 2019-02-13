package com.pharbers.common.calc

import com.pharbers.pactions.actionbase._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, _}

object phMaxPersistentAction {
    def apply[T](args: pActionArgs = NULLArgs): pActionTrait = new phMaxPersistentAction[T](args)
}

class phMaxPersistentAction[T](override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "max_persistent_action"

    override def perform(prMap: pActionArgs): pActionArgs = {

        val prod_name = defaultArgs.asInstanceOf[MapArgs].get("prod_name").asInstanceOf[ListArgs].get.map(_.asInstanceOf[StringArgs].get)
        val max_name = defaultArgs.asInstanceOf[MapArgs].get("max_name").asInstanceOf[StringArgs].get
        val max_path = defaultArgs.asInstanceOf[MapArgs].get("max_path").asInstanceOf[StringArgs].get
//        val max_delimiter = defaultArgs.asInstanceOf[MapArgs].get
//                .getOrElse("max_delimiter", StringArgs(31.toChar.toString)).asInstanceOf[StringArgs].get

        val maxDF = prMap.asInstanceOf[MapArgs].get("max_calc_action").asInstanceOf[DFArgs].get

        val resultLocation = max_path + max_name

        val condition = prod_name.map(x => col("Product") like s"%$x%").reduce((a, b) => a or b) //获得所有子公司
        val max_result = maxDF.withColumn("belong2company", when(condition, 1).otherwise(0))

        max_result.write.mode(SaveMode.Append)
                .option("header", value = true)
                .parquet(resultLocation)
        StringArgs(max_name)
    }
}