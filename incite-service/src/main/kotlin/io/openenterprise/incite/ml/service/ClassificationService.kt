package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.ClassificationFunction
import io.openenterprise.incite.data.domain.Classification
import io.openenterprise.incite.data.domain.LogisticRegression
import io.openenterprise.service.AbstractMutableEntityService
import io.openenterprise.spark.sql.DatasetUtils
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.persistence.EntityNotFoundException
import kotlin.jvm.Throws

interface ClassificationService : AbstractService<Classification, ClassificationFunction>,
    AbstractMutableEntityService<Classification, String> {

    companion object : AbstractService.BaseCompanionObject() {

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

        @JvmStatic
        @QuerySqlFunction(alias = "classification_predict")
        fun predict(id: String, jsonOrSql: String): String {
            val classificationService = getBean(ClassificationService::class.java)
            val classification = classificationService.retrieve(id)
                ?: throw EntityNotFoundException("Classification with ID, $id, is not found")

            val dataset = classificationService.predict(jsonOrSql, classification)

            return DatasetUtils.toJson(dataset)
        }
    }
}