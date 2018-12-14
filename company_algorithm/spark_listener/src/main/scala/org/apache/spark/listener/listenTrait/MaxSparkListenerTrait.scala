package org.apache.spark.listener.listenTrait

import org.apache.spark.scheduler._

trait MaxSparkListenerTrait extends SparkListener {
    val job_name: String
}
