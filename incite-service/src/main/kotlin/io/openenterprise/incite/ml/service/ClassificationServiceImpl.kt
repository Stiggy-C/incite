package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.Classification
import io.openenterprise.incite.data.domain.LogisticRegression
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.incite.service.PipelineServiceImpl
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.ml.classification.Classifier
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
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
open class ClassificationServiceImpl(
    @Inject private val datasetService: DatasetService,
    @Inject private val pipelineService: PipelineService,
    @Inject private val transactionTemplate: TransactionTemplate
) :
    ClassificationService,
    AbstractMachineLearningServiceImpl<Classification>(
        datasetService,
        pipelineService
    ) {

    override fun <M : Model<M>> train(entity: Classification): M {
        val dataset = getAggregatedDataset(entity)

        @Suppress("UNCHECKED_CAST")
        return when (entity.algorithm) {
            is LogisticRegression -> {
                val logisticRegression = entity.algorithm as LogisticRegression

                buildLogisticRegressionModel(
                    dataset,
                    logisticRegression.featureColumns.stream().collect(Collectors.joining(",")),
                    logisticRegression.labelColumn,
                    logisticRegression.elasticNetMixing,
                    logisticRegression.maxIterations,
                    logisticRegression.regularization
                )
            }
            else ->
                throw UnsupportedOperationException()
        } as M
    }

    override fun persistModel(entity: Classification, sparkModel: MLWritable): UUID {
        val modelId = putToCache(sparkModel)
        val model = Classification.Model()
        model.id = modelId.toString()

        entity.models.add(model)

        transactionTemplate.execute {
            update(entity)
        }

        return modelId
    }

    override fun predict(entity: Classification, jsonOrSql: String): Dataset<Row> {
        if (entity.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        assert(pipelineService is PipelineServiceImpl)

        val model = entity.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val sparkModel: Model<*> = when (entity.algorithm) {
            is LogisticRegression -> {
                getFromCache<LogisticRegressionModel>(UUID.fromString(model.id))
            }
            else ->
                throw UnsupportedOperationException()
        }

        val dataset = predict(sparkModel, jsonOrSql)

        datasetService.write(dataset, entity.sinks, false)

        return dataset
    }

    private fun buildLogisticRegressionModel(
        dataset: Dataset<Row>,
        featuresColumns: String,
        labelColumn: String,
        elasticNetMixing: Double,
        maxIteration: Int,
        regularization: Double
    ): LogisticRegressionModel {
        val logisticRegression = org.apache.spark.ml.classification.LogisticRegression()
        logisticRegression.elasticNetParam = elasticNetMixing
        logisticRegression.maxIter = maxIteration
        logisticRegression.regParam = regularization

        return buildSparkModel(logisticRegression, dataset, labelColumn, StringUtils.split(featuresColumns, ","))
    }

    private fun <A : Classifier<Vector, *, M>, M : ClassificationModel<Vector, M>> buildSparkModel(
        algorithm: A,
        dataset: Dataset<Row>,
        labelColumn: String,
        featuresColumns: Array<String>
    ): M {
        @Suppress("unchecked_cast")
        val transformedDataset0 =
            StringIndexer().setInputCol(labelColumn).setOutputCol("label").fit(dataset).transform(dataset)
        val transformedDataset1 =
            VectorAssembler().setInputCols(featuresColumns).setOutputCol(((algorithm as HasFeaturesCol).featuresCol))
                .transform(transformedDataset0)

        return algorithm.fit(transformedDataset1)
    }
}