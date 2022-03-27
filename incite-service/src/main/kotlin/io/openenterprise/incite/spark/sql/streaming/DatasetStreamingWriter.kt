package io.openenterprise.incite.spark.sql.streaming

import io.openenterprise.incite.spark.sql.AbstractDatasetWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.StreamingQuery

class DatasetStreamingWriter(dataStreamWriter: DataStreamWriter<*>, val streamingQuery: StreamingQuery) :
    AbstractDatasetWriter<DataStreamWriter<*>>(dataStreamWriter)