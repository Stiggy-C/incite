package io.openenterprise.ignite.spark

import io.openenterprise.springframework.context.ApplicationContextUtils
import org.apache.ignite.Ignite
import org.apache.spark.SparkContext


class IgniteContext(sparkContext: SparkContext) :
    org.apache.ignite.spark.IgniteContext(sparkContext) {

    override fun ignite(): Ignite {
        return ApplicationContextUtils.getApplicationContext()!!.getBean(Ignite::class.java)
    }
}