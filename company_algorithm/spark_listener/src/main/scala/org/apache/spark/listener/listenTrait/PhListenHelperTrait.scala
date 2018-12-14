package org.apache.spark.listener.listenTrait

trait PhListenHelperTrait {
    def jobStart(taskSum: Int): Unit
    def jobEnd(): Unit
    def taskEnd(listener: MaxSparkListenerTrait): Unit
}
