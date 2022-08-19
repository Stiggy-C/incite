package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.AlternatingLeastSquares
import io.openenterprise.incite.data.domain.Recommendation
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.IgniteException
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*
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
            val algorithm = mergeParamsIntoAlgorithm(
                Recommendation.SupportedAlgorithm.valueOf(algo).clazz.getDeclaredConstructor()
                    .newInstance() as Recommendation.Algorithm, algoSpecificParams
            )
            val recommendation = setUpMachineLearning(
                RecommendationService::class.java,
                Recommendation(),
                algorithm,
                sourceSql,
                sinkTable,
                primaryKeyColumns
            )

            getBean(RecommendationService::class.java).create(recommendation)

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
        @Throws(IgniteException::class)
        @QuerySqlFunction(alias = "build_recommendation_model")
        fun train(id: String): UUID {
            val recommendationService = getBean(RecommendationService::class.java)
            val collaborativeFiltering = recommendationService.retrieve(id)
                ?: throw IgniteException(EntityNotFoundException("Recommendation (ID $id) is not found"))
            val sparkModel = when (collaborativeFiltering.algorithm) {
                is AlternatingLeastSquares -> recommendationService.train<ALSModel>(collaborativeFiltering)
                else -> throw IgniteException(UnsupportedOperationException())
            }

            return recommendationService.persistModel(collaborativeFiltering, sparkModel)
        }
    }

    fun recommendForAllUsers(recommendation: Recommendation, numberOfItems: Int): Dataset<Row>

    fun recommendForUsersSubset(recommendation: Recommendation, jsonOrSql: String, numberOfItems: Int): Dataset<Row>
}