package io.openenterprise.incite.ml.service

import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.incite.data.domain.IgniteSink
import io.openenterprise.incite.data.domain.JdbcSource
import io.openenterprise.incite.data.domain.Recommendation
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonValue
import javax.persistence.EntityNotFoundException

interface RecommendationService : MachineLearningService<Recommendation>,
    AbstractMutableEntityService<Recommendation, String> {

    companion object : MachineLearningService.BaseCompanionObject() {

        /**
         * Perform classification defined by the given [io.openenterprise.incite.data.domain.Recommendation]
         * with the latest [Recommendation.Model] if there is any and write the result to the given sinks
         * defined in the given [Recommendation].
         *
         * @param id The [UUID] of [Recommendation] as [String]
         * @return Number of entries in the result
         * @throws EntityNotFoundException If no such [Recommendation]
         */
        @JvmStatic
        @QuerySqlFunction(alias = "recommendation_predict")
        fun predict(id: String, jsonOrSql: String): Long {
            val recommendationService = getBean(RecommendationService::class.java)
            val collaborativeFiltering = recommendationService.retrieve(id)
                ?: throw EntityNotFoundException("Classification with ID, $id, is not found")
            val result = recommendationService.predict(collaborativeFiltering, jsonOrSql)

            writeToSinks(result, collaborativeFiltering.sinks)

            return result.count()
        }

        @JvmStatic
        @QuerySqlFunction(alias = "set_up_recommendation")
        fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID {
            val objectMapper = getBean(ObjectMapper::class.java)

            var algorithm =
                Recommendation.Algorithm.Supported.valueOf(algo).clazz.newInstance() as Recommendation.Algorithm
            var algorithmAsJsonObject: JsonValue = objectMapper.convertValue(algorithm, JsonObject::class.java)
            val algorithmSpecificParamsAsJsonObject = objectMapper.readValue(algoSpecificParams, JsonObject::class.java)
            val jsonMergePatch = Json.createMergePatch(algorithmSpecificParamsAsJsonObject)

            algorithmAsJsonObject = jsonMergePatch.apply(algorithmAsJsonObject)
            algorithm = objectMapper.convertValue(algorithmAsJsonObject, algorithm::class.java)

            val recommendationService = getBean(RecommendationService::class.java) as RecommendationServiceImpl

            val embeddedIgniteRdbmsDatabase = recommendationService.buildEmbeddedIgniteRdbmsDatabase()

            val jdbcSource = JdbcSource()
            jdbcSource.rdbmsDatabase = embeddedIgniteRdbmsDatabase
            jdbcSource.query = sourceSql

            val jdbcSink = IgniteSink()
            jdbcSink.rdbmsDatabase = embeddedIgniteRdbmsDatabase
            jdbcSink.table = sinkTable
            jdbcSink.primaryKeyColumns = primaryKeyColumns

            val recommendation = Recommendation()
            recommendation.algorithm = algorithm
            recommendation.sources = mutableListOf(jdbcSource)
            recommendation.sinks = mutableListOf(jdbcSink)

            recommendationService.create(recommendation)

            return UUID.fromString(recommendation.id)
        }

        /**
         * Train a model for the given [Recommendation] if there is such an entity.
         *
         * @param id The [UUID] of [Recommendation] as [String]
         * @return The [UUID] of [Recommendation.Model]
         * @throws EntityNotFoundException If no such [Recommendation]
         */
        @JvmStatic
        @Throws(EntityNotFoundException::class)
        @QuerySqlFunction(alias = "build_recommendation_model")
        fun train(id: String): UUID {
            val recommendationService = getBean(RecommendationService::class.java)
            val collaborativeFiltering = recommendationService.retrieve(id)
                ?: throw EntityNotFoundException("CollaborativeFiltering with ID, $id, is not found")
            val sparkModel: ALSModel = recommendationService.train(collaborativeFiltering)

            return recommendationService.persistModel(collaborativeFiltering, sparkModel)
        }
    }

    fun recommendForAllUsers(recommendation: Recommendation, numberOfItems: Int): Dataset<Row>

    fun recommendForUsersSubset(recommendation: Recommendation, jsonOrSql: String, numberOfItems: Int): Dataset<Row>
}