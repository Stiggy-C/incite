package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.BisectingKMeans
import io.openenterprise.incite.data.domain.Clustering
import io.openenterprise.incite.data.domain.KMeans
import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.incite.service.PipelineServiceImpl
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.shared.HasFeaturesCol
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.EntityNotFoundException

@Named
open class ClusteringServiceImpl(
    @Inject private val datasetService: DatasetService,
    @Inject private val pipelineService: PipelineService
) : AbstractMachineLearningServiceImpl<Clustering, Clustering.Model, Clustering.Algorithm>(
    datasetService,
    pipelineService
),
    ClusteringService {

    /*override fun persistModel(entity: Clustering, sparkModel: MLWritable): UUID {
        val modelId = putToS3(sparkModel)
        val model = Clustering.Model()
        model.id = modelId.toString()

        entity.models.add(model)

        transactionTemplate.execute {
            update(entity)
        }

        return modelId
    }*/

    /*override fun predict(entity: Clustering, jsonOrSql: String): Dataset<Row> {
        if (entity.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        assert(pipelineService is PipelineServiceImpl)

        val model = entity.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val sparkModel: Model<*> =
            when (entity.algorithm) {
                is BisectingKMeans -> getFromS3(UUID.fromString(model.id), BisectingKMeansModel::class.java)
                is KMeans -> getFromS3(UUID.fromString(model.id), KMeansModel::class.java)
                else -> throw UnsupportedOperationException()
            }

        val dataset = postProcessLoadedDataset(entity.algorithm, sparkModel, loadDataset(jsonOrSql))
        val result = predict(sparkModel, dataset)

        datasetService.write(result, entity.sinks, false)

        return result
    }*/

    /*override fun <M : Model<M>> train(entity: Clustering): M {
        val dataset = getAggregatedDataset(entity)

        @Suppress("UNCHECKED_CAST")
        return when (val algorithm = entity.algorithm) {
            is Clustering.FeatureColumnsBasedAlgorithm -> {
                when (algorithm) {
                    is BisectingKMeans -> {
                        buildBisectingKMeansModel(
                            dataset,
                            algorithm.featureColumns.stream().collect((Collectors.joining(","))),
                            algorithm.k,
                            algorithm.maxIterations,
                            algorithm.seed
                        )
                    }
                    is KMeans -> {
                        buildKMeansModel(
                            dataset,
                            algorithm.featureColumns.stream().collect((Collectors.joining(","))),
                            algorithm.k,
                            algorithm.maxIterations,
                            algorithm.seed
                        )
                    }
                    else -> throw UnsupportedOperationException()
                }
            }
            else -> throw UnsupportedOperationException()
        } as M
    }*/

    @Suppress("UNCHECKED_CAST")
    override fun <SM : Model<SM>> buildSparkModel(entity: Clustering, dataset: Dataset<Row>): SM =
        when (val algorithm = entity.algorithm) {
            is Clustering.FeatureColumnsBasedAlgorithm -> {
                when (algorithm) {
                    is BisectingKMeans -> {
                        buildBisectingKMeansModel(
                            dataset,
                            algorithm.featureColumns.stream().collect((Collectors.joining(","))),
                            algorithm.k,
                            algorithm.maxIterations,
                            algorithm.seed
                        )
                    }

                    is KMeans -> {
                        buildKMeansModel(
                            dataset,
                            algorithm.featureColumns.stream().collect((Collectors.joining(","))),
                            algorithm.k,
                            algorithm.maxIterations,
                            algorithm.seed
                        )
                    }

                    else -> throw UnsupportedOperationException()
                }
            }

            else -> throw UnsupportedOperationException()
        } as SM

    override fun getSparkModel(algorithm: MachineLearning.Algorithm, modelId: String): Model<*> =
        when (algorithm) {
            is BisectingKMeans -> getFromS3(UUID.fromString(modelId), BisectingKMeansModel::class.java)
            is KMeans -> getFromS3(UUID.fromString(modelId), KMeansModel::class.java)
            else -> throw UnsupportedOperationException()
        }

    override fun <A : MachineLearning.Algorithm, M : Model<*>> postProcessLoadedDataset(
        algorithm: A,
        model: M,
        dataset: Dataset<Row>
    ): Dataset<Row> = when (algorithm) {
        is Clustering.FeatureColumnsBasedAlgorithm -> {
            val featuresColumns = algorithm.featureColumns

            VectorAssembler().setInputCols(featuresColumns.toTypedArray())
                .setOutputCol(((model as HasFeaturesCol).featuresCol))
                .transform(dataset)
        }

        else -> throw UnsupportedOperationException()
    }

    private fun buildBisectingKMeansModel(
        dataset: Dataset<Row>,
        featuresColumns: String,
        k: Int,
        maxIteration: Int,
        seed: Long
    ): BisectingKMeansModel {
        val bisectingKMeans = org.apache.spark.ml.clustering.BisectingKMeans()
        bisectingKMeans.k = k
        bisectingKMeans.maxIter = maxIteration
        bisectingKMeans.seed = seed

        return buildSparkModel(bisectingKMeans, dataset, StringUtils.split(featuresColumns, ","))
    }

    private fun buildKMeansModel(
        dataset: Dataset<Row>,
        featuresColumns: String,
        k: Int,
        maxIteration: Int,
        seed: Long
    ): KMeansModel {
        val kMeans = org.apache.spark.ml.clustering.KMeans()
        kMeans.k = k
        kMeans.maxIter = maxIteration
        kMeans.seed = seed

        return buildSparkModel(kMeans, dataset, StringUtils.split(featuresColumns, ","))
    }

    private fun <A : Estimator<M>, M : Model<M>> buildSparkModel(
        algorithm: A,
        dataset: Dataset<Row>,
        featuresColumns: Array<String>
    ): M {
        assert(algorithm is HasFeaturesCol)

        @Suppress("unchecked_cast")
        val transformedDataset =
            VectorAssembler().setInputCols(featuresColumns).setOutputCol(((algorithm as HasFeaturesCol).featuresCol))
                .transform(dataset)

        return algorithm.fit(transformedDataset)
    }
}