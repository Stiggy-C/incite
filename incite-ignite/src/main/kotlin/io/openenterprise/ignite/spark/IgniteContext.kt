package io.openenterprise.ignite.spark

import org.apache.ignite.Ignite
import org.apache.spark.SparkContext

class IgniteContext(private val ignite: Ignite, sparkContext: SparkContext) :
    org.apache.ignite.spark.IgniteContext(sparkContext) {

    override fun ignite(): Ignite = ignite
}