package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.ClassificationFunction
import io.openenterprise.incite.data.domain.Classification
import io.openenterprise.incite.data.domain.LogisticRegression
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.LogisticRegressionModel
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
class ClassificationServiceImpl(
    @Inject private val aggregateService: AggregateService,
    @Inject private val datasetService: DatasetService,
    @Inject private val classificationFunction: ClassificationFunction,
    @Inject private val transactionTemplate: TransactionTemplate
) :
    ClassificationService,
    AbstractMachineLearningServiceImpl<Classification, ClassificationFunction>(
        aggregateService,
        datasetService,
        classificationFunction
    ) {

    override fun <M : Model<M>> buildModel(entity: Classification): M {
        val dataset = getAggregatedDataset(entity)

        @Suppress("UNCHECKED_CAST")
        return when (entity.algorithm) {
            is LogisticRegression -> {
                val logisticRegression = entity.algorithm as LogisticRegression

                classificationFunction.buildLogisticRegressionModel(
                    dataset,
                    if (logisticRegression.family == null) null else logisticRegression.family!!.name.lowercase(),
                    logisticRegression.featureColumns.stream().collect(Collectors.joining(",")),
                    logisticRegression.labelColumn,
                    logisticRegression.elasticNetMixing,
                    logisticRegression.maxIteration,
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

        assert(aggregateService is AggregateServiceImpl)

        val model = entity.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val sparkModel: Model<*> = when (entity.algorithm) {
            is LogisticRegression -> {
                getFromCache<LogisticRegressionModel>(UUID.fromString(model.id))
            }
            else ->
                throw UnsupportedOperationException()
        }

        val dataset = classificationFunction.predict(sparkModel, jsonOrSql)

        datasetService.write(dataset, entity.sinks, false)

        return dataset
    }
}