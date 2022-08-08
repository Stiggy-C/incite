package io.openenterprise.incite

import com.google.common.collect.ImmutableMap
import io.openenterprise.incite.spark.sql.WriterHolder
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.time.OffsetDateTime

data class PipelineContext(

    /**
     * Same as io.openenterprise.incite.data.domain.Pipeline.id
     */
    var id: String,

    var dataset: Dataset<Row>? = null,

    var startDateTime: OffsetDateTime? = null,

    var status: Status? = null,

    var variables: ImmutableMap<String, Any>,

    var writerHolders: Set<WriterHolder<*>>? = null
) {

    enum class Status {

        FAILED, PROCESSING, STOPPED

    }
}