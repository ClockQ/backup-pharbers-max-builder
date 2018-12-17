package com.pharbers.nhwa

import scala.reflect.ClassTag
import com.pharbers.pactions.actionbase._

object phResult2StringJob {

    val str2StrTranFun = SingleArgFuncArgs{ arg: pActionArgs =>
        arg.asInstanceOf[StringArgs]
    }

    val lst2StrTranFun = SingleArgFuncArgs{ arg: pActionArgs =>
        StringArgs(arg.asInstanceOf[ListArgs].get.map(x => x.asInstanceOf[StringArgs].get).mkString("#"))
    }

    def apply[T: ClassTag](prName: String, args: pActionArgs): pActionTrait = {
        new phResult2StringJob[T](prName, args)
    }
}

class phResult2StringJob[T: ClassTag](prName: String, override val defaultArgs: pActionArgs) extends pActionTrait {

    override val name: String = "result"

    override def perform(pr: pActionArgs): pActionArgs =
        defaultArgs.asInstanceOf[SingleArgFuncArgs[pActionArgs, StringArgs]].get(pr.asInstanceOf[MapArgs].get(prName))
}