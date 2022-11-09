package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.LinearRegression
import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.data.domain.Regression
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.Model
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*
import javax.inject.Inject
import javax.inject.Named

@Named
open class RegressionServiceImpl(
    @Inject private val datasetService: DatasetService,
    @Inject private val pipelineService: PipelineService
) : AbstractMachineLearningServiceImpl<Regression, Regression.Model, Regression.Algorithm>(
    datasetService,
    pipelineService
),
    RegressionService {

    @Suppress("UNCHECKED_CAST")
    override fun <SM : Model<SM>> buildSparkModel(entity: Regression, dataset: Dataset<Row>): SM =
        when (val algorithm = entity.algorithm) {
            is LinearRegression -> buildLinearRegression(dataset, algorithm.epsilon, algorithm.maxIterations)
            else -> throw UnsupportedOperationException()
        } as SM

    override fun getSparkModel(algorithm: MachineLearning.Algorithm, modelId: String): Model<*> =
        when (algorithm) {
            is LinearRegression -> getFromS3(UUID.fromString(modelId), LinearRegressionModel::class.java)
            else -> throw UnsupportedOperationException()
        }

    private fun buildLinearRegression(
        dataset: Dataset<Row>,
        epsilon: Double,
        maxIterations: Int
    ): LinearRegressionModel {
        val linearRegression = org.apache.spark.ml.regression.LinearRegression()
        linearRegression.epsilon = epsilon
        linearRegression.maxIter = maxIterations

        return linearRegression.fit(dataset)
    }
}