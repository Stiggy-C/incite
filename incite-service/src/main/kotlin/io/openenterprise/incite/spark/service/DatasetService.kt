package io.openenterprise.incite.spark.service

import io.openenterprise.incite.data.domain.NonStreamingSink
import io.openenterprise.incite.data.domain.Source
import io.openenterprise.incite.data.domain.StreamingSink
import io.openenterprise.incite.spark.sql.DatasetNonStreamingWriter
import io.openenterprise.incite.spark.sql.streaming.DatasetStreamingWriter
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

interface DatasetService {

    fun load(source: Source): Dataset<Row>

    fun write(dataset: Dataset<Row>, sink: StreamingSink): DatasetStreamingWriter

    fun write(dataset: Dataset<Row>, sink: NonStreamingSink): DatasetNonStreamingWriter
}