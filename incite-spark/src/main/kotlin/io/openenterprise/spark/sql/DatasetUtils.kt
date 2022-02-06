package io.openenterprise.spark.sql

import org.apache.spark.sql.Dataset
import java.util.*
import java.util.stream.Collectors

class DatasetUtils {

    companion object {

        @JvmStatic
        val jsonStringsMap = HashMap<UUID, StringBuilder>()

        @JvmStatic
        fun <T> toJson(dataset: Dataset<T>): String {
            val session = UUID.randomUUID()
            jsonStringsMap[session] = StringBuilder()
            jsonStringsMap[session]!!.append("[")

            dataset.repartition(200).toJSON().foreachPartition {
                jsonStringsMap[session]!!.append(it.asSequence().toList().stream().collect(Collectors.joining(",")))
                jsonStringsMap[session]!!.append(",")
            }

            val lastCommaIndex = jsonStringsMap[session]!!.lastIndexOf(",")
            if (lastCommaIndex == jsonStringsMap[session]!!.length - 1) {
                jsonStringsMap[session]!!.deleteCharAt(lastCommaIndex)
            }

            jsonStringsMap[session]!!.append("]")

            val jsonString = jsonStringsMap[session]!!.toString()

            jsonStringsMap.remove(session)

            return jsonString
        }
    }
}