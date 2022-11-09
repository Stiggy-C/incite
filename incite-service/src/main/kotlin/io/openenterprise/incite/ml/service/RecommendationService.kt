package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.data.domain.Recommendation
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.IgniteException
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*
import javax.persistence.EntityNotFoundException

interface RecommendationService :
    MachineLearningService<Recommendation, Recommendation.Algorithm, Recommendation.Model>,
    AbstractMutableEntityService<Recommendation, String> {

    companion object :
        MachineLearningService.BaseCompanionObject<Recommendation, Recommendation.Algorithm, Recommendation.Model, RecommendationService>() {

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
        override fun predict(id: String, jsonOrSql: String): Long = super.predict(id, jsonOrSql)

        @JvmStatic
        @QuerySqlFunction(alias = "set_up_recommendation")
        override fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID = super.setUp(algo, algoSpecificParams, sourceSql, sinkTable, primaryKeyColumns)

        /**
         * Train a model for the given [Recommendation] if there is such an entity.
         *
         * @param id The [UUID] of [Recommendation] as [String]
         * @return The [UUID] of [Recommendation.Model]
         * @throws EntityNotFoundException If no such [Recommendation]
         */
        @JvmStatic
        @Throws(IgniteException::class)
        @QuerySqlFunction(alias = "train_recommendation_model")
        override fun train(id: String): UUID = super.train(id)
        override fun getMachineLearningClass(): Class<Recommendation> = Recommendation::class.java

        override fun getMachineLearningAlgorithmClass(algo: String): Class<out MachineLearning.Algorithm> =
            Recommendation.SupportedAlgorithm.valueOf(algo).clazz

        override fun getMachineLearningService(): RecommendationService = getBean(RecommendationService::class.java)
    }

    fun recommendForAllUsers(recommendation: Recommendation, numberOfItems: Int): Dataset<Row>

    fun recommendForUsersSubset(recommendation: Recommendation, jsonOrSql: String, numberOfItems: Int): Dataset<Row>
}