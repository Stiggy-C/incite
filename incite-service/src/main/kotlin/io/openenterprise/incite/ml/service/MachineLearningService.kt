package io.openenterprise.incite.ml.service

import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.ignite.cache.query.Function
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.IgniteException
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonValue
import javax.persistence.EntityNotFoundException

interface MachineLearningService<T : MachineLearning<A, M>, A : MachineLearning.Algorithm, M : MachineLearning.Model<M>> :
    AbstractMutableEntityService<T, String> {

    fun <M : Model<M>> getFromS3(modelId: UUID, clazz: Class<M>): M

    fun persistModel(entity: T, sparkModel: MLWritable): UUID

    fun predict(entity: T, jsonOrSql: String): Dataset<Row>

    fun putToS3(model: MLWritable): UUID

    fun <SM : Model<SM>> train(entity: T): SM

    abstract class BaseCompanionObject<T : MachineLearning<A, M>, A : MachineLearning.Algorithm, M : MachineLearning.Model<M>, S : MachineLearningService<T, A, M>> :
        Function.BaseCompanionObject() {

        open fun predict(id: String, jsonOrSql: String): Long {
            val machineLearningService = getMachineLearningService()
            val entity = machineLearningService.retrieve(id)
                ?: throw EntityNotFoundException("MachineLearning with ID, $id, is not found")
            val result = machineLearningService.predict(entity, jsonOrSql)

            writeToSinks(result, entity.sinks)

            return result.count()
        }

        open fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID {
            val algorithm: A = instantiateMachineLearningAlgorithm(algo, algoSpecificParams)
            val machineLearning: T = setUpMachineLearning(
                getMachineLearningClass().getDeclaredConstructor().newInstance(),
                algorithm,
                sourceSql,
                sinkTable,
                primaryKeyColumns
            )

            getMachineLearningService().create(machineLearning)

            return UUID.fromString(machineLearning.id)
        }

        open fun train(id: String): UUID {
            val machineLearningService = getMachineLearningService()
            val entity = machineLearningService.retrieve(id)
                ?: throw IgniteException(EntityNotFoundException("ClusterAnalysis (ID: $id) is not found"))
            val sparkModel: Model<*> = machineLearningService.train(entity)

            return machineLearningService.persistModel(entity, sparkModel as MLWritable)
        }

        protected abstract fun getMachineLearningClass(): Class<T>

        protected abstract fun getMachineLearningAlgorithmClass(algo: String): Class<out MachineLearning.Algorithm>

        protected abstract fun getMachineLearningService(): S

        @Suppress("UNCHECKED_CAST")
        protected open fun <A : MachineLearning.Algorithm> instantiateMachineLearningAlgorithm(
            algo: String,
            algoSpecificParams: String
        ): A = mergeParamsIntoAlgorithm(
            getMachineLearningAlgorithmClass(algo).getDeclaredConstructor().newInstance() as A,
            algoSpecificParams
        )

        protected fun <T> mergeParamsIntoAlgorithm(algorithm: T, paramsJsonString: String): T {
            val objectMapper = getBean(ObjectMapper::class.java)

            var algorithmAsJsonObject: JsonValue = objectMapper.convertValue(algorithm, JsonObject::class.java)
            val algorithmSpecificParamsAsJsonObject = objectMapper.readValue(paramsJsonString, JsonObject::class.java)
            val jsonMergePatch = Json.createMergePatch(algorithmSpecificParamsAsJsonObject)

            algorithmAsJsonObject = jsonMergePatch.apply(algorithmAsJsonObject)

            return objectMapper.convertValue(algorithmAsJsonObject, algorithm!!::class.java)
        }

        protected fun <T : MachineLearning<A, M>, M : MachineLearning.Model<M>, A : MachineLearning.Algorithm> setUpMachineLearning(
            machineLearning: T,
            algorithm: A,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): T {
            @Suppress("UNCHECKED_CAST")
            val machineLearningService = getMachineLearningService() as AbstractMachineLearningServiceImpl<T, M, A>
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