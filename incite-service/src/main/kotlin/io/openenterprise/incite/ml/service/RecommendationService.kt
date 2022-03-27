package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.RecommendationFunction
import io.openenterprise.incite.data.domain.Recommendation
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.persistence.EntityNotFoundException

interface RecommendationService : AbstractMLService<Recommendation, RecommendationFunction>,
    AbstractMutableEntityService<Recommendation, String> {

    companion object : AbstractMLService.BaseCompanionObject() {

        /**
         * Build a model for the given [io.openenterprise.incite.data.domain.Recommendation] if there is such an
         * entity.
         *
         * @param id The [UUID] of [Recommendation] as [String]
         * @return The [UUID] of [Recommendation.Model]
         * @throws EntityNotFoundException If no such [Recommendation]
         */
        @JvmStatic
        @Throws(EntityNotFoundException::class)
        @QuerySqlFunction(alias = "build_recommendation_model")
        fun buildModel(id: String): UUID {
            val recommendationService = getBean(RecommendationService::class.java)
            val transactionTemplate = getBean(TransactionTemplate::class.java)

            val collaborativeFiltering = recommendationService.retrieve(id)
                ?: throw EntityNotFoundException("CollaborativeFiltering with ID, $id, is not found")
            val sparkModel: ALSModel = recommendationService.buildModel(collaborativeFiltering)

            val modelId = recommendationService.putToCache(sparkModel)
            val model = Recommendation.Model()
            model.id = modelId.toString()

            collaborativeFiltering.models.add(model)

            transactionTemplate.execute {
                recommendationService.update(collaborativeFiltering)
            }

            return modelId
        }

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
    }

    fun recommendForAllUsers(recommendation: Recommendation, numberOfItems: Int): Dataset<Row>

    fun recommendForUsersSubset(
        recommendation: Recommendation,
        jsonOrSql: String,
        numberOfItems: Int
    ): Dataset<Row>
}