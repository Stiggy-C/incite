package io.openenterprise.spark.sql

import org.apache.spark.sql.Dataset
import java.util.*
import java.util.stream.Collectors
import java.util.stream.StreamSupport

class DatasetUtils {

    companion object {

        @JvmStatic
        fun toJson(dataset: Dataset<*>): String {
            val result = StringBuilder()
            result.append("[")

            if (dataset.count() > 0) {
                val numberOfPartitions = when (dataset.count()) {
                    in 0..10000 -> 1
                    in 10001 .. 100000 -> 5
                    in 100001 .. 1000000 -> 10
                    in 1000001 .. 10000000 -> 20
                    in 10000001 .. 100000000 -> 40
                    else -> 100
                }
                dataset.toJSON().repartition(numberOfPartitions).foreachPartition {
                    val jsonString = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false
                    )
                        .collect(Collectors.joining(","))

                    result.append("$jsonString,")
                }

                result.dropLast(1)
            }

            result.append("]")

            return result.toString()
        }
    }
}