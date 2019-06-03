package com.pharbers.common.calc

import com.pharbers.pactions.jobs.sequenceJob
import com.pharbers.channel.detail.channelEntity
import com.pharbers.pactions.actionbase.pActionTrait
import com.pharbers.pactions.generalactions.readCsvAction

case class phCommonMaxJobByCsv(args: Map[String, String])
                              (override implicit val send: channelEntity => Unit) extends phCommonMaxJobTrait {
    override lazy val loadPanelData: sequenceJob = new sequenceJob {
        override val name: String = "panel_data"
        override val actions: List[pActionTrait] = ???
//            readCsvAction(panel_file, delimiter = panel_delimiter, applicationName = job_id) :: Nil
    }
}