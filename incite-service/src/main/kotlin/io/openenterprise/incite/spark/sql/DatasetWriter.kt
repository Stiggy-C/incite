package io.openenterprise.incite.spark.sql

import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row

interface DatasetWriter<T>

abstract class AbstractDatasetWriter<T>(open val writer: T) : DatasetWriter<T>

class DatasetNonStreamingWriter(dataFrameWriter: DataFrameWriter<Row>) :
    AbstractDatasetWriter<DataFrameWriter<Row>>(dataFrameWriter)