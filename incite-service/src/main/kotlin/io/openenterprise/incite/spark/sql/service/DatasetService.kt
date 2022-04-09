package io.openenterprise.incite.spark.sql.service

import io.openenterprise.incite.data.domain.NonStreamingSink
import io.openenterprise.incite.data.domain.Sink
import io.openenterprise.incite.data.domain.Source
import io.openenterprise.incite.data.domain.StreamingSink
import io.openenterprise.incite.spark.sql.DatasetNonStreamingWriter
import io.openenterprise.incite.spark.sql.DatasetWriter
import io.openenterprise.incite.spark.sql.streaming.DatasetStreamingWriter
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

interface DatasetService {

    fun load(source: Source): Dataset<Row>

    fun load(source: Source, variables: Map<String, *>): Dataset<Row>

    fun load(sources: List<Source>): List<Dataset<Row>>

    fun load(sources: List<Source>, variables: Map<String, *>): List<Dataset<Row>>

    fun write(dataset: Dataset<Row>, sink: StreamingSink): DatasetStreamingWriter

    fun write(dataset: Dataset<Row>, sink: NonStreamingSink): DatasetNonStreamingWriter

    fun write(dataset: Dataset<Row>, sinks: List<Sink>, forceStreaming: Boolean): Set<DatasetWriter<*>>
}