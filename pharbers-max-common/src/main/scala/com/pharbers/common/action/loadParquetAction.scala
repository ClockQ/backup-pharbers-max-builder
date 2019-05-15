package com.pharbers.common.action

import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.jobs.sequenceJob
import com.pharbers.pactions.actionbase.pActionTrait
import com.pharbers.pactions.generalactions.readParquetAction

case class loadParquetAction(override val name: String, file: String,
                             delimiter: String = 31.toChar.toString)
                            (implicit val sparkDriver: phSparkDriver) extends sequenceJob {
    override val actions: List[pActionTrait] = readParquetAction(file, name) :: Nil
}
