package io.openenterprise.incite.spark.sql

import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row

interface WriterHolder<T>

abstract class AbstractWriterHolder<T>(open val writer: T) : WriterHolder<T>

class DataFrameWriterHolder(dataFrameWriter: DataFrameWriter<*>) :
    AbstractWriterHolder<DataFrameWriter<*>>(dataFrameWriter)