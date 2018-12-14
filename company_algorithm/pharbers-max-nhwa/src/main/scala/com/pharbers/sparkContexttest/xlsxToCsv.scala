package com.pharbers.sparkContexttest

import akka.actor.Actor
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, StringArgs, pActionTrait}
import com.pharbers.pactions.excel.input.PhExcelXLSXCommonFormat
import com.pharbers.pactions.generalactions._
import com.pharbers.pactions.jobs.{choiceJob, sequenceJob, sequenceJobWithMap}
import com.pharbers.sparkContexttest.spark_conn_instance_test.{conf, spark_sql_context}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import com.pharbers.spark.phSparkDriver

case class xlsxToCsv(args: Map[String, String])
                    (implicit _actor: Actor) extends sequenceJobWithMap {
    override val name = "xlsxToCsvJob"
    val temp_dir = "hdfs:///workData/cache/"
    val universe_file = "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_Universe_INF.xlsx"
    
    val load_universe_file: sequenceJob = new sequenceJob {
        override val name: String = "universe_file"
        override val actions: List[pActionTrait] =
            xlsxReadingAction[PhExcelXLSXCommonFormat](universe_file, "universe") ::
                    saveCurrenResultAction(temp_dir + "universe") ::
                    csv2DFAction(temp_dir + "universe") :: Nil
    }
    
    val df = MapArgs(
        Map(
            "ym" -> StringArgs("1804")
        )
    )
    
    override val actions: List[pActionTrait] = {
        load_universe_file ::
                showJob(df) ::
                Nil
    }
    StringArgs(actions.toString())
}
