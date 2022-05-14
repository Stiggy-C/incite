package io.openenterprise.incite.ml.service

import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.incite.data.domain.Classification
import io.openenterprise.incite.data.domain.IgniteSink
import io.openenterprise.incite.data.domain.JdbcSource
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonValue
import javax.persistence.EntityNotFoundException

interface ClassificationService : MachineLearningService<Classification>,
    AbstractMutableEntityService<Classification, String> {

    companion object : MachineLearningService.BaseCompanionObject() {

        /**
         * Make predictions for the given [Classification] with the latest [Classification.Model] if there is any and
         * write the result to the given sinks defined in the given [Classification].
         *
         * @param id The [UUID] of [Classification] as [String]
         * @return Number of entries in the result
         * @throws EntityNotFoundException If no such [Classification]
         */
        @JvmStatic
        @QuerySqlFunction(alias = "classification_predict")
        fun predict(id: String, jsonOrSql: String): Long {
            val classificationService = getBean(ClassificationService::class.java)
            val classification = classificationService.retrieve(id)
                ?: throw EntityNotFoundException("Classification with ID, $id, is not found")
            val result = classificationService.predict(classification, jsonOrSql)

            writeToSinks(result, classification.sinks)

            return result.count()
        }

        @JvmStatic
        @QuerySqlFunction(alias = "set_up_classification")
        fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID {
            val objectMapper = getBean(ObjectMapper::class.java)

            var algorithm =
                Classification.SupportedAlgorithm.valueOf(algo).clazz.newInstance() as Classification.Algorithm
            var algorithmAsJsonObject: JsonValue = objectMapper.convertValue(algorithm, JsonObject::class.java)
            val algorithmSpecificParamsAsJsonObject = objectMapper.readValue(algoSpecificParams, JsonObject::class.java)
            val jsonMergePatch = Json.createMergePatch(algorithmSpecificParamsAsJsonObject)

            algorithmAsJsonObject = jsonMergePatch.apply(algorithmAsJsonObject)
            algorithm = objectMapper.convertValue(algorithmAsJsonObject, algorithm::class.java)

            val classificationService = getBean(ClassificationService::class.java) as ClassificationServiceImpl

            val embeddedIgniteRdbmsDatabase = classificationService.buildEmbeddedIgniteRdbmsDatabase()

            val jdbcSource = JdbcSource()
            jdbcSource.rdbmsDatabase = embeddedIgniteRdbmsDatabase
            jdbcSource.query = sourceSql

            val jdbcSink = IgniteSink()
            jdbcSink.rdbmsDatabase = embeddedIgniteRdbmsDatabase
            jdbcSink.table = sinkTable
            jdbcSink.primaryKeyColumns = primaryKeyColumns

            val classification = Classification()
            classification.algorithm = algorithm as Classification.Algorithm
            classification.sources = mutableListOf(jdbcSource)
            classification.sinks = mutableListOf(jdbcSink)

            classificationService.create(classification)

            return UUID.fromString(classification.id)
        }

        /**
         * Train a model for the given [Classification] if there is such an entity.
         *
         * @param id The [UUID] of [Classification] as [String]
         * @return The [UUID] of [Classification.Model]
         * @throws EntityNotFoundException If no such [Classification]
         */
        @JvmStatic
        @Throws(EntityNotFoundException::class)
        @QuerySqlFunction(alias = "train_classification_model")
        fun train(id: String): UUID {
            val classificationService = getBean(ClassificationService::class.java)
            val classification = classificationService.retrieve(id)
                ?: throw EntityNotFoundException("Classification with ID, $id, is not found")
            val sparkModel: Model<*> = classificationService.train(classification)

            return classificationService.persistModel(classification, sparkModel as MLWritable)
        }
    }
}