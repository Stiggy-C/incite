package io.openenterprise.incite

import io.openenterprise.incite.spark.sql.DatasetWriter
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

data class AggregateContext(var status: Status) {

    lateinit var dataset: Dataset<*>

    lateinit var datasetWriters: Set<DatasetWriter<*>>

    enum class Status {

        PROCESSING, STOPPED
    }
}