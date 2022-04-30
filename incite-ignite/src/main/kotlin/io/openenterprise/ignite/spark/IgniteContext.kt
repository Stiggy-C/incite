package io.openenterprise.ignite.spark

import io.openenterprise.springframework.context.ApplicationContextUtils
import org.apache.ignite.Ignite
import org.apache.spark.SparkContext
import org.springframework.context.ApplicationContext

class IgniteContext(sparkContext: SparkContext) :
    org.apache.ignite.spark.IgniteContext(sparkContext) {

    override fun ignite(): Ignite =
        ApplicationContextUtils.getApplicationContext()!!.getBean(Ignite::class.java)
}