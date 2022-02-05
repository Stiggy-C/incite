package io.openenterprise.incite

import io.openenterprise.incite.spark.sql.DatasetWriter
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

class AggregateContext() {

    constructor(dataset: Dataset<Row>, datasetWriters: Set<DatasetWriter<*>>): this() {
        this.dataset = dataset
        this.datasetWriters = datasetWriters
    }

    /**
     * The resulting org.apache.spark.sql.Dataset after aggregate is being triggered.
     */
    lateinit var dataset: Dataset<Row>

    lateinit var datasetWriters: Set<DatasetWriter<*>>
}



