package io.openenterprise.incite.ml.service

import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.ignite.cache.query.Function
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonValue

interface MachineLearningService<T : MachineLearning<*, *>> {

    fun <M : Model<M>> getFromCache(modelId: UUID): M

    fun persistModel(entity: T, sparkModel: MLWritable): UUID

    fun predict(entity: T, jsonOrSql: String): Dataset<Row>

    fun putToCache(model: MLWritable): UUID

    fun <M : Model<M>> train(entity: T): M

    abstract class BaseCompanionObject : Function.BaseCompanionObject() {

        protected fun <T> mergeParamsIntoAlgorithm(algorithm: T, paramsJsonString: String): T {
            val objectMapper = getBean(ObjectMapper::class.java)

            var algorithmAsJsonObject: JsonValue = objectMapper.convertValue(algorithm, JsonObject::class.java)
            val algorithmSpecificParamsAsJsonObject = objectMapper.readValue(paramsJsonString, JsonObject::class.java)
            val jsonMergePatch = Json.createMergePatch(algorithmSpecificParamsAsJsonObject)

            algorithmAsJsonObject = jsonMergePatch.apply(algorithmAsJsonObject)

            return objectMapper.convertValue(algorithmAsJsonObject, algorithm!!::class.java)
        }

        protected fun <C : MachineLearningService<T>, T : MachineLearning<A, *>, A: MachineLearning.Algorithm> setUpMachineLearning(
            serviceClass: Class<C>,
            machineLearning: T,
            algorithm: A,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): T {
            @Suppress("UNCHECKED_CAST")
            val machineLearningService = getBean(serviceClass) as AbstractMachineLearningServiceImpl<T>
            val embeddedIgniteRdbmsDatabase = machineLearningService.buildEmbeddedIgniteRdbmsDatabase()
            val (source, sink) = buildSourceAndSink(
                embeddedIgniteRdbmsDatabase,
                sourceSql,
                sinkTable,
                primaryKeyColumns
            )

            machineLearning.algorithm = algorithm
            machineLearning.sources = mutableListOf(source)
            machineLearning.sinks = mutableListOf(sink)

            return machineLearning
        }

        protected fun writeToSinks(dataset: Dataset<Row>, sinks: List<Sink>) =
            getBean(DatasetService::class.java).write(dataset, sinks, false)

        private fun buildSourceAndSink(
            rdbmsDatabase: RdbmsDatabase,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): Pair<Source, Sink> {
            val jdbcSource = JdbcSource()
            jdbcSource.rdbmsDatabase = rdbmsDatabase
            jdbcSource.query = sourceSql

            val jdbcSink = IgniteSink()
            jdbcSink.rdbmsDatabase = rdbmsDatabase
            jdbcSink.table = sinkTable
            jdbcSink.primaryKeyColumns = primaryKeyColumns

            return Pair(jdbcSource, jdbcSink)
        }
    }
}