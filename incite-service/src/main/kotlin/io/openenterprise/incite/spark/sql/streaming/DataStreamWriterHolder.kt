package io.openenterprise.incite.spark.sql.streaming

import io.openenterprise.incite.spark.sql.AbstractWriterHolder
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.StreamingQuery

class DataStreamWriterHolder(dataStreamWriter: DataStreamWriter<*>, val streamingQuery: StreamingQuery) :
    AbstractWriterHolder<DataStreamWriter<*>>(dataStreamWriter)