package io.openenterprise.spark.sql

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.util.*
import java.util.stream.Collectors

sealed class DatasetUtils {

    companion object {

        @JvmStatic
        val jsonStringsMap = HashMap<UUID, StringBuilder>()

        @JvmStatic
        fun load(
            sparkSession: SparkSession,
            sql: String,
            driverClass: String,
            url: String,
            username: String,
            password: String
        ): Dataset<Row> {
            return sparkSession.read()
                .format("jdbc")
                .option("query", sql)
                .option("driver", driverClass)
                .option("url", url)
                .option("user", username)
                .option("password", password)
                .load()
        }

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