package com.pharbers.unitTest.action

import akka.actor.ActorSystem
import org.apache.spark.sql.DataFrame
import com.pharbers.pactions.actionbase._

object writeCheckResultAction {
    def apply(args: pActionArgs)(implicit as: ActorSystem): pActionTrait = new writeCheckResultAction(args)
}

class writeCheckResultAction(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "checkResult"

    override def perform(prMap: pActionArgs): pActionArgs = {
        val action = defaultArgs.asInstanceOf[MapArgs].get("checkAction").asInstanceOf[PhActionArgs].get
        val totalResult: DataFrame = prMap.asInstanceOf[MapArgs].get("resultCheckAction").asInstanceOf[DFArgs].get

        val test_path = action.unitTestConf.get.head.test_path
        val test_name = action.unitTestConf.get.head.test_name

        totalResult.show(false)

        totalResult.coalesce(1).write
                .format("csv")
                .option("header", value = true)
                .option("delimiter", "#")
                .save(test_path + test_name)
        println("单个市场单元测试：" + test_name)
        StringArgs(test_name)
    }
}
