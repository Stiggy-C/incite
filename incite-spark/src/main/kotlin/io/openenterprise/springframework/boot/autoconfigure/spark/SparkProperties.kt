package io.openenterprise.springframework.boot.autoconfigure.spark

import org.springframework.boot.context.properties.ConfigurationProperties
import java.util.*

@ConfigurationProperties("spark")
class SparkProperties {

    var appName: String? = null

    var executor: Properties = Properties()

    var hadoop: Properties = Properties()

    var master: String? = null

    var memory: Properties = Properties()

    var sql: Properties = Properties()
}