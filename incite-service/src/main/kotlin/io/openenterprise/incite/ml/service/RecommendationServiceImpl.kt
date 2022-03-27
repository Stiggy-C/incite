package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.RecommendationFunction
import io.openenterprise.incite.data.domain.AlternatingLeastSquares
import io.openenterprise.incite.data.domain.Recommendation
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
import org.apache.spark.ml.Model
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.EntityNotFoundException

@Named
class RecommendationServiceImpl(
    @Inject private val aggregateService: AggregateService,
    @Inject private val recommendationFunction: RecommendationFunction
) : RecommendationService,
    AbstractMLServiceImpl<Recommendation, String, RecommendationFunction>(
        aggregateService,
        recommendationFunction
    ) {

    override fun recommendForAllUsers(
        recommendation: Recommendation,
        numberOfItems: Int
    ): Dataset<Row> {
        if (recommendation.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        assert(aggregateService is AggregateServiceImpl)

        val model = recommendation.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val result = when (recommendation.algorithm) {
            is AlternatingLeastSquares -> {
                val alsModel: ALSModel = getFromCache(UUID.fromString(model.id))

                recommendationFunction.recommendForAllUsers(alsModel, numberOfItems)
            }
            else -> throw UnsupportedOperationException()
        }

        (aggregateService as AggregateServiceImpl).writeSinks(result, recommendation.sinks, false)

        return result
    }

    override fun recommendForUsersSubset(
        recommendation: Recommendation,
        jsonOrSql: String,
        numberOfItems: Int
    ): Dataset<Row> {
        if (recommendation.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        assert(aggregateService is AggregateServiceImpl)

        val model = recommendation.models.stream().findFirst().orElseThrow { EntityNotFoundException() }

        val result = when (recommendation.algorithm) {
            is AlternatingLeastSquares -> {
                val alsModel: ALSModel = getFromCache(UUID.fromString(model.id))

                recommendationFunction.recommendForUsersSubset(alsModel, jsonOrSql, numberOfItems)
            }
            else -> throw UnsupportedOperationException()
        }

        (aggregateService as AggregateServiceImpl).writeSinks(result, recommendation.sinks, false)

        return result

    }

    override fun <M : Model<M>> buildModel(entity: Recommendation): M {
        val dataset = getAggregatedDataset(entity)

        @Suppress("UNCHECKED_CAST")
        return when (val algorithm = entity.algorithm) {
            is AlternatingLeastSquares -> {
                recommendationFunction.buildAlsModel(
                    dataset,
                    algorithm.implicitPreference,
                    algorithm.maxIteration,
                    algorithm.numberOfItemBlocks,
                    algorithm.numberOfUserBlocks,
                    algorithm.regularization
                )
            }
            else -> throw UnsupportedOperationException()
        } as M
    }

    override fun predict(entity: Recommendation, jsonOrSql: String): Dataset<Row> {
        if (entity.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        assert(aggregateService is AggregateServiceImpl)

        val model = entity.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val sparkModel: Model<*> =  when (val algorithm = entity.algorithm) {
            is AlternatingLeastSquares -> getFromCache<ALSModel>(UUID.fromString(model.id))
            else -> throw UnsupportedOperationException()
        }

        val result = recommendationFunction.predict(sparkModel, jsonOrSql)

        (aggregateService as AggregateServiceImpl).writeSinks(result, entity.sinks, false)

        return result
    }
}