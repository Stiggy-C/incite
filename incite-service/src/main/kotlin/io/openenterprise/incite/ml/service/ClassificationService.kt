package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.Classification
import io.openenterprise.incite.data.domain.LogisticRegression
import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.IgniteException
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.util.MLWritable
import java.util.*
import javax.persistence.EntityNotFoundException

interface ClassificationService :
    MachineLearningService<Classification, Classification.Algorithm, Classification.Model>,
    AbstractMutableEntityService<Classification, String> {

    companion object :
        MachineLearningService.BaseCompanionObject<Classification, Classification.Algorithm, Classification.Model, ClassificationService>() {

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
        override fun predict(id: String, jsonOrSql: String): Long = super.predict(id, jsonOrSql)

        @JvmStatic
        @QuerySqlFunction(alias = "set_up_classification")
        override fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID = super.setUp(algo, algoSpecificParams, sourceSql, sinkTable, primaryKeyColumns)

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
        override fun train(id: String): UUID = super.train(id)

        override fun getMachineLearningClass(): Class<Classification> = Classification::class.java

        override fun getMachineLearningAlgorithmClass(algo: String): Class<out MachineLearning.Algorithm> =
            Classification.SupportedAlgorithm.valueOf(algo).clazz

        override fun getMachineLearningService(): ClassificationService = getBean(ClassificationService::class.java)
    }
}