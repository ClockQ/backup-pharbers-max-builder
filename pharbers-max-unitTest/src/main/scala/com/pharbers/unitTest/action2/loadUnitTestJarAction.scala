//package com.pharbers.unitTest.action2
//
//import com.pharbers.pactions.actionbase._
//import com.pharbers.spark.phSparkDriver
//
//case class loadUnitTestJarAction(override val defaultArgs : pActionArgs = NULLArgs) extends pActionTrait {
//    override val name: String = "load_unitTest_jar"
//
//    override def perform(args : pActionArgs): pActionArgs = {
//
//        val sparkDriver = phSparkDriver()
//        sparkDriver.sc.addJar("hdfs:///workData/jars/pharbers-unitTest-0.1.jar")
//        sparkDriver.sc.addJar("hdfs:///workData/jars/pharbers-max-0.1.jar")
//        NULLArgs
//    }
//}
