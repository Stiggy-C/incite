package io.openenterprise.incite

import io.openenterprise.incite.spark.sql.DatasetWriter
import org.apache.spark.sql.Dataset

data class PipelineContext(
    var status: Status,

    var dataset: Dataset<*>? = null,

    var datasetWriters: Set<DatasetWriter<*>>? = null
) {
    enum class Status {

        PROCESSING, STOPPED
    }
}