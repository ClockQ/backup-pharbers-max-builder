package com.pharbers.spark.listener.listenTrait

trait PhListenHelperTrait {
    def jobStart(taskSum: Int): Unit
    def taskEnd(): Unit
    def jobEnd(app_name: String, listener: MaxSparkListenerTrait): Unit
}
