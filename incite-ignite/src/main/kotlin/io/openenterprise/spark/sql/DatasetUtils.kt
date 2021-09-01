package io.openenterprise.spark.sql

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.io.ByteArrayOutputStream

class DatasetUtils {

    companion object {

        @JvmStatic
        fun toJson(dataset: Dataset<Row>): String {
            val byteArrayOutputStream = ByteArrayOutputStream()

            IOUtils.write("[", byteArrayOutputStream, Charsets.UTF_8)
            dataset.toJSON().collectAsList().forEach {
                IOUtils.write(it, byteArrayOutputStream, Charsets.UTF_8)
            }
            IOUtils.write("]", byteArrayOutputStream, Charsets.UTF_8)

            return byteArrayOutputStream.toString(Charsets.UTF_8)
        }

    }
}