package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.FPGrowth
import io.openenterprise.incite.data.domain.FrequentPatternMining
import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.Model
import org.apache.spark.ml.fpm.FPGrowthModel
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.EntityNotFoundException

@Named
open class FrequentPatternMiningServiceImpl(
    @Inject private val datasetService: DatasetService,
    @Inject private val pipelineService: PipelineService
) : AbstractMachineLearningServiceImpl<FrequentPatternMining, FrequentPatternMining.Model, FrequentPatternMining.Algorithm>(
    datasetService,
    pipelineService
), FrequentPatternMiningService {

    /*override fun persistModel(entity: FrequentPatternMining, sparkModel: MLWritable): UUID {
        val modelId = putToS3(sparkModel)
        val model = FrequentPatternMining.Model()
        model.id = modelId.toString()

        entity.models.add(model)

        transactionTemplate.execute {
            update(entity)
        }

        return modelId
    }*/

    /*override fun predict(entity: FrequentPatternMining, jsonOrSql: String): Dataset<Row> {
        if (entity.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        val model = entity.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val sparkModel: Model<*> = when (entity.algorithm) {
            is FPGrowth -> getFromS3(UUID.fromString(model.id), FPGrowthModel::class.java)
            else -> throw UnsupportedOperationException()
        }

        val dataset = postProcessLoadedDataset(entity.algorithm, sparkModel, loadDataset(jsonOrSql))
        val result = predict(sparkModel, dataset)

        datasetService.write(result, entity.sinks, false)

        return result
    }*/

    /*override fun <M : Model<M>> train(entity: FrequentPatternMining): M {
        val dataset = postProcessLoadedDataset(entity.algorithm, getAggregatedDataset(entity))

        @Suppress("UNCHECKED_CAST")
        return when (entity.algorithm) {
            is FPGrowth -> {
                val fpGrowth = entity.algorithm as FPGrowth

                buildFpGrowthModel(dataset, fpGrowth.itemsColumn, fpGrowth.minConfidence, fpGrowth.minSupport)
            }
            else -> throw UnsupportedOperationException()
        } as M
    }*/

    @Suppress("UNCHECKED_CAST")
    override fun <SM : Model<SM>> buildSparkModel(entity: FrequentPatternMining, dataset: Dataset<Row>): SM =
        when (entity.algorithm) {
            is FPGrowth -> {
                val fpGrowth = entity.algorithm as FPGrowth

                buildFpGrowthModel(dataset, fpGrowth.itemsColumn, fpGrowth.minConfidence, fpGrowth.minSupport)
            }

            else -> throw UnsupportedOperationException()
        } as SM

    override fun getSparkModel(algorithm: MachineLearning.Algorithm, modelId: String): Model<*> =
        when (algorithm) {
            is FPGrowth -> getFromS3(UUID.fromString(modelId), FPGrowthModel::class.java)
            else -> throw UnsupportedOperationException()
        }

    override fun <A : MachineLearning.Algorithm> postProcessLoadedDataset(
        algorithm: A,
        dataset: Dataset<Row>
    ): Dataset<Row> {
        return when (algorithm) {
            is FPGrowth -> {
                val itemsColField = dataset.schema().fields()[dataset.schema().fieldIndex(algorithm.itemsColumn)]

                if (itemsColField.dataType() is ArrayType)
                    dataset
                else {
                    // Need to convert itemsCol to arrayType:
                    val selects = dataset.schema().fields().asList().stream().map {
                        if (it.name() == algorithm.itemsColumn) {
                            "array(split(`${it.name()}`, ' ')) as `${it.name()}`"
                        } else {
                            it.name()
                        }
                    }.toArray { size -> Array(size) { "" } }

                    dataset.selectExpr(*selects)
                }
            }

            else -> throw UnsupportedOperationException()
        }
    }

    override fun <M : Model<M>> postProcessLoadedDataset(model: Model<M>, dataset: Dataset<Row>): Dataset<Row> {
        return when (model) {
            is FPGrowthModel -> {
                val itemsColField = dataset.schema().fields()[dataset.schema().fieldIndex(model.itemsCol)]

                if (itemsColField.dataType() is ArrayType)
                    dataset
                else {
                    // Need to convert itemsCol to arrayType:
                    val selects = dataset.schema().fields().asList().stream().map {
                        if (it.name() == model.itemsCol) {
                            "array(split(`${it.name()}`, ' ')) as `${it.name()}`"
                        } else {
                            it.name()
                        }
                    }.toArray { size -> Array(size) { "" } }

                    dataset.selectExpr(*selects)
                }
            }

            else -> throw UnsupportedOperationException()
        }
    }

    private fun buildFpGrowthModel(
        dataset: Dataset<Row>,
        itemsColumn: String,
        minConfidence: Double,
        minSupport: Double
    ): FPGrowthModel {
        val fpGrowth = org.apache.spark.ml.fpm.FPGrowth()
        fpGrowth.itemsCol = itemsColumn
        fpGrowth.minConfidence = minConfidence
        fpGrowth.minSupport = minSupport

        return fpGrowth.fit(dataset)
    }
}