package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.RecommendationFunction
import io.openenterprise.incite.data.domain.AlternatingLeastSquares
import io.openenterprise.incite.data.domain.Recommendation
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.Model
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.EntityNotFoundException

@Named
class RecommendationServiceImpl(
    @Inject private val aggregateService: AggregateService,
    @Inject private val datasetService: DatasetService,
    @Inject private val recommendationFunction: RecommendationFunction,
    @Inject private val transactionTemplate: TransactionTemplate
) : RecommendationService,
    AbstractMachineLearningServiceImpl<Recommendation, RecommendationFunction>(
        aggregateService,
        datasetService,
        recommendationFunction
    ) {

    override fun recommendForAllUsers(
        recommendation: Recommendation,
        numberOfItems: Int
    ): Dataset<Row> {
        if (recommendation.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        val model = recommendation.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val result = when (recommendation.algorithm) {
            is AlternatingLeastSquares -> {
                val alsModel: ALSModel = getFromCache(UUID.fromString(model.id))

                recommendationFunction.recommendForAllUsers(alsModel, numberOfItems)
            }
            else -> throw UnsupportedOperationException()
        }

        datasetService.write(result, recommendation.sinks, false)

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
        val model = recommendation.models.stream().findFirst().orElseThrow { EntityNotFoundException() }

        val result = when (recommendation.algorithm) {
            is AlternatingLeastSquares -> {
                val alsModel: ALSModel = getFromCache(UUID.fromString(model.id))

                recommendationFunction.recommendForUsersSubset(alsModel, jsonOrSql, numberOfItems)
            }
            else -> throw UnsupportedOperationException()
        }

        datasetService.write(result, recommendation.sinks, false)

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

    override fun persistModel(entity: Recommendation, sparkModel: MLWritable): UUID {
        val modelId = putToCache(sparkModel)
        val model = Recommendation.Model()
        model.id = modelId.toString()

        entity.models.add(model)

        transactionTemplate.execute {
            update(entity)
        }

        return modelId
    }

    override fun predict(entity: Recommendation, jsonOrSql: String): Dataset<Row> {
        if (entity.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        val model = entity.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val sparkModel: Model<*> =  when (val algorithm = entity.algorithm) {
            is AlternatingLeastSquares -> getFromCache<ALSModel>(UUID.fromString(model.id))
            else -> throw UnsupportedOperationException()
        }

        val result = recommendationFunction.predict(sparkModel, jsonOrSql)

        datasetService.write(result, entity.sinks, false)

        return result
    }
}