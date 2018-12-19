package com.pharbers.common.action

import com.pharbers.pactions.actionbase.pActionTrait
import com.pharbers.pactions.generalactions.readCsvAction
import com.pharbers.pactions.jobs.sequenceJob

case class readCpa(cpa_file: String, job_id: String) extends sequenceJob {
    override val name = "cpa"
    override val actions: List[pActionTrait] = readCsvAction(arg_path = cpa_file, applicationName = job_id) :: Nil
}
