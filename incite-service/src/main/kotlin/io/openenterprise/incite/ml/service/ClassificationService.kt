package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.Classification
import io.openenterprise.incite.data.domain.LogisticRegression
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.IgniteException
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.util.MLWritable
import java.util.*
import javax.persistence.EntityNotFoundException

interface ClassificationService : MachineLearningService<Classification>,
    AbstractMutableEntityService<Classification, String> {

    companion object : MachineLearningService.BaseCompanionObject() {

        /**
         * Make predictions for the given [Classification] with the latest [Classification.Model] if there is any and
         * write the result to the given sinks defined in the given [Classification].
         *
         * @param id The [UUID] of [Classification] as [String]
         * @return Number of entries in the result
         * @throws EntityNotFoundException If no such [Classification]
         */
        @JvmStatic
        @QuerySqlFunction(alias = "classification_predict")
        fun predict(id: String, jsonOrSql: String): Long {
            val classificationService = getBean(ClassificationService::class.java)
            val classification = classificationService.retrieve(id)
                ?: throw EntityNotFoundException("Classification with ID, $id, is not found")
            val result = classificationService.predict(classification, jsonOrSql)

            writeToSinks(result, classification.sinks)

            return result.count()
        }

        @JvmStatic
        @QuerySqlFunction(alias = "set_up_classification")
        fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID {
            val algorithm: Classification.Algorithm = mergeParamsIntoAlgorithm(
                Classification.SupportedAlgorithm.valueOf(algo).clazz.getDeclaredConstructor()
                    .newInstance() as Classification.Algorithm,
                algoSpecificParams
            )
            val classification = setUpMachineLearning(
                ClassificationService::class.java,
                Classification(),
                algorithm,
                sourceSql,
                sinkTable,
                primaryKeyColumns
            )

            getBean(ClassificationService::class.java).create(classification)

            return UUID.fromString(classification.id)
        }

        /**
         * Train a model for the given [Classification] if there is such an entity.
         *
         * @param id The [UUID] of [Classification] as [String]
         * @return The [UUID] of [Classification.Model]
         * @throws EntityNotFoundException If no such [Classification]
         */
        @JvmStatic
        @Throws(IgniteException::class)
        @QuerySqlFunction(alias = "train_classification_model")
        fun train(id: String): UUID {
            val classificationService = getBean(ClassificationService::class.java)
            val classification = classificationService.retrieve(id)
                ?: throw IgniteException(EntityNotFoundException("Classification (ID: $id) is not found"))
            val sparkModel = when (classification.algorithm) {
                is LogisticRegression -> classificationService.train<LogisticRegressionModel>(classification)
                else -> throw IgniteException(UnsupportedOperationException())
            }

            return classificationService.persistModel(classification, sparkModel as MLWritable)
        }
    }
}