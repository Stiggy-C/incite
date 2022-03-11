package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.ClassificationFunction
import io.openenterprise.incite.data.domain.Classification
import io.openenterprise.incite.data.domain.Clustering
import io.openenterprise.incite.data.domain.LogisticRegression
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.persistence.EntityNotFoundException
import kotlin.jvm.Throws

interface ClassificationService : AbstractService<Classification, ClassificationFunction>,
    AbstractMutableEntityService<Classification, String> {

    companion object : AbstractService.BaseCompanionObject() {

        /**
         * Build a model for the given [io.openenterprise.incite.data.domain.Classification] if there is such an entity.
         *
         * @param id The [java.util.UUID] of [io.openenterprise.incite.data.domain.Classification] as [java.lang.String]
         * @return The [java.util.UUID] of [Classification.Model]
         * @throws EntityNotFoundException If no such [io.openenterprise.incite.data.domain.Classification]
         */
        @JvmStatic
        @Throws(EntityNotFoundException::class)
        @QuerySqlFunction(alias = "build_classification_model")
        fun buildModel(id: String): UUID {
            val classificationService = getBean(ClassificationService::class.java)
            val transactionTemplate = getBean(TransactionTemplate::class.java)

            val classification = classificationService.retrieve(id)
                ?: throw EntityNotFoundException("Classification with ID, $id, is not found")

            val sparkModel = when (classification.algorithm) {
                is LogisticRegression -> classificationService.buildModel<LogisticRegressionModel>(classification)
                else -> throw UnsupportedOperationException()
            }

            val modelId = classificationService.putToCache(sparkModel)
            val model = Classification.Model()
            model.id = modelId.toString()

            classification.models.add(model)

            transactionTemplate.execute {
                classificationService.update(classification)
            }

            return modelId
        }

        /**
         * Perform classification defined by the given [io.openenterprise.incite.data.domain.Classification] with the
         * latest [io.openenterprise.incite.data.domain.Classification.Model] if there is any and write the result to the
         * given sinks defined in the given [io.openenterprise.incite.data.domain.Clustering]
         *
         * @param id The [java.util.UUID] of [io.openenterprise.incite.data.domain.Classification] as [java.lang.String]
         * @return Number of entries in the result
         * @throws EntityNotFoundException If no such [io.openenterprise.incite.data.domain.Classification]
         */
        @JvmStatic
        @QuerySqlFunction(alias = "classification_predict")
        fun predict(id: String, jsonOrSql: String): Long {
            val classificationService = getBean(ClassificationService::class.java)
            val classification = classificationService.retrieve(id)
                ?: throw EntityNotFoundException("Classification with ID, $id, is not found")
            val result = classificationService.predict(jsonOrSql, classification)

            writeToSinks(result, classification.sinks)

            return result.count()
        }
    }
}