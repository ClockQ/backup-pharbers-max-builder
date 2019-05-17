package com.pharbers.common.resultexport

import com.pharbers.pactions.generalactions.readParquetAction
import com.pharbers.pactions.actionbase.{MapArgs, NULLArgs, StringArgs, pActionArgs, pActionTrait}

object phLoadMaxResultAction {
    def apply(args: pActionArgs = NULLArgs): pActionTrait = new phLoadMaxResultAction(args)
}

class phLoadMaxResultAction(override val defaultArgs: pActionArgs) extends pActionTrait {
    override val name: String = "phLoadMaxResultAction"

    override def perform(pr: pActionArgs): pActionArgs = {

        val job_id = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get
        val max_path = defaultArgs.asInstanceOf[MapArgs].get("max_path").asInstanceOf[StringArgs].get
        val max_name = defaultArgs.asInstanceOf[MapArgs].get("max_name").asInstanceOf[StringArgs].get

        ???
//        readParquetAction(
//            arg_path = max_path + max_name,
//            applicationName = job_id
//        ).perform(NULLArgs)
    }
}
