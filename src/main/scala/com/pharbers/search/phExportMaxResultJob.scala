package com.pharbers.search

import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionTrait}
import com.pharbers.pactions.generalactions.setLogLevelAction
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.search.actions._


object phExportMaxResultJob {
    def apply(args: Map[String, String]): phExportMaxResultJob = {
        new phExportMaxResultJob {
            override lazy val company: String = args.getOrElse("company", throw new Exception("Illegal company"))
            override lazy val ym: String = args.getOrElse("ym", throw new Exception("no ym"))
            override lazy val mkt: String = args.getOrElse("mkt", throw new Exception("no mkt"))
            override lazy val job_id: String = args.getOrElse("job_id", throw new Exception("no job_id"))
        }
    }
}

trait phExportMaxResultJob extends sequenceJobWithMap {
    override val name: String = "phExportMaxResultJob"

    val company: String
    val ym: String
    val mkt: String
    val job_id: String

    lazy val searchArgs = MapArgs(
        Map(
            "job_id" -> StringArgs(job_id),
            "company" -> StringArgs(company),
            "ym" -> StringArgs(ym),
            "mkt" -> StringArgs(mkt)
        )
    )

    override val actions: List[pActionTrait] = setLogLevelAction("ERROR", job_id) ::
        phReadMaxResultAction(searchArgs) ::
        phExportMaxResultAction(searchArgs) ::
        Nil
}

