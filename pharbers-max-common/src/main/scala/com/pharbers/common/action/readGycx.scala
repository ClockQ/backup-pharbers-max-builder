package com.pharbers.common.action

import com.pharbers.pactions.actionbase.pActionTrait
import com.pharbers.pactions.generalactions.readCsvAction
import com.pharbers.pactions.jobs.sequenceJob

case class readGycx(gycx_file: String, job_id: String) extends sequenceJob {
    override val name = "gycx"
    override val actions: List[pActionTrait] = readCsvAction(arg_path = gycx_file, applicationName = job_id) :: Nil
}
