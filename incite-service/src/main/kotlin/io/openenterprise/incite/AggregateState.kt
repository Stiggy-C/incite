package io.openenterprise.incite

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.io.Serializable
import java.time.OffsetDateTime

class AggregateState(): Serializable {

    constructor(dataset: Dataset<Row>, isActive: Boolean, isStreaming: Boolean, lastRunDateTime: OffsetDateTime): this() {
        this.dataset = dataset
        this.isActive = isActive
        this.isStreaming = isStreaming
        this.lastRunDateTime = lastRunDateTime
    }

    /**
     * The resulting org.apache.spark.sql.Dataset after aggregate is being triggered.
     */
    lateinit var dataset: Dataset<Row>

    var isActive: Boolean = false

    var isStreaming: Boolean = false

    lateinit var lastRunDateTime: OffsetDateTime
}