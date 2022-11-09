package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.AlternatingLeastSquares
import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.data.domain.Recommendation
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.Model
import org.apache.spark.ml.recommendation.ALS
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
open class RecommendationServiceImpl(
    @Inject private val datasetService: DatasetService,
    @Inject private val pipelineService: PipelineService
) : AbstractMachineLearningServiceImpl<Recommendation, Recommendation.Model, Recommendation.Algorithm>(
    datasetService,
    pipelineService
),
    RecommendationService {

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
                val alsModel = getFromS3(UUID.fromString(model.id), ALSModel::class.java)

                recommendForAllUsers(alsModel, numberOfItems)
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
                val alsModel = getFromS3(UUID.fromString(model.id), ALSModel::class.java)

                recommendForUsersSubset(alsModel, jsonOrSql, numberOfItems)
            }

            else -> throw UnsupportedOperationException()
        }

        datasetService.write(result, recommendation.sinks, false)

        return result
    }

    /*override fun persistModel(entity: Recommendation, sparkModel: MLWritable): UUID {
        val modelId = putToS3(sparkModel)
        val model = Recommendation.Model()
        model.id = modelId.toString()

        entity.models.add(model)

        transactionTemplate.execute {
            update(entity)
        }

        return modelId
    }*/

    /*override fun predict(entity: Recommendation, jsonOrSql: String): Dataset<Row> {
        if (entity.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        val model = entity.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val sparkModel: Model<*> = when (val algorithm = entity.algorithm) {
            is AlternatingLeastSquares -> getFromS3(UUID.fromString(model.id), ALSModel::class.java)
            else -> throw UnsupportedOperationException()
        }

        val dataset = postProcessLoadedDataset(entity.algorithm, sparkModel, loadDataset(jsonOrSql))
        val result = predict(sparkModel, dataset)

        datasetService.write(result, entity.sinks, false)

        return result
    }*/

    /*override fun <M : Model<M>> train(entity: Recommendation): M {
        val dataset = getAggregatedDataset(entity)

        @Suppress("UNCHECKED_CAST")
        return when (val algorithm = entity.algorithm) {
            is AlternatingLeastSquares -> {
                buildAlsModel(
                    dataset,
                    algorithm.implicitPreference,
                    algorithm.itemColumn,
                    algorithm.maxIteration,
                    algorithm.numberOfItemBlocks,
                    algorithm.numberOfUserBlocks,
                    algorithm.regularization
                )
            }
            else -> throw UnsupportedOperationException()
        } as M
    }*/

    @Suppress("UNCHECKED_CAST")
    override fun <SM : Model<SM>> buildSparkModel(entity: Recommendation, dataset: Dataset<Row>): SM =
        when (val algorithm = entity.algorithm) {
            is AlternatingLeastSquares -> {
                buildAlsModel(
                    dataset,
                    algorithm.implicitPreference,
                    algorithm.itemColumn,
                    algorithm.maxIteration,
                    algorithm.numberOfItemBlocks,
                    algorithm.numberOfUserBlocks,
                    algorithm.regularization
                )
            }

            else -> throw UnsupportedOperationException()
        } as SM

    override fun getSparkModel(algorithm: MachineLearning.Algorithm, modelId: String): Model<*> =
        when (algorithm) {
            is AlternatingLeastSquares -> getFromS3(UUID.fromString(modelId), ALSModel::class.java)
            else -> throw UnsupportedOperationException()
        }

    private fun buildAlsModel(
        dataset: Dataset<Row>,
        implicitPreference: Boolean = false,
        itemColumn: String = AlternatingLeastSquares.ITEM_COLUMN_DEFAULT,
        maxIteration: Int = 10,
        numberOfItemBlocks: Int = 10,
        numberOfUserBlocks: Int = 10,
        regularization: Double = 1.0
    ): ALSModel {
        val als = ALS()
        als.implicitPrefs = implicitPreference
        als.itemCol = itemColumn
        als.maxIter = maxIteration
        als.numItemBlocks = numberOfItemBlocks
        als.numUserBlocks = numberOfUserBlocks
        als.regParam = regularization

        return als.fit(dataset)
    }

    private fun recommendForAllUsers(alsModel: ALSModel, numberOfItems: Int): Dataset<Row> =
        alsModel.recommendForAllUsers(numberOfItems)

    private fun recommendForUsersSubset(alsModel: ALSModel, jsonOrSql: String, numberOfItems: Int): Dataset<Row> {
        val dataset = if (isJson(jsonOrSql)) {
            loadDatasetFromJson(jsonOrSql)
        } else {
            loadDatasetFromSql(jsonOrSql)
        }

        return alsModel.recommendForUserSubset(dataset, numberOfItems)
    }
}