package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.ClassificationFunction
import io.openenterprise.incite.data.domain.Classification
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import java.util.*
import javax.persistence.EntityNotFoundException
import kotlin.jvm.Throws

interface ClassificationService : MachineLearningService<Classification, ClassificationFunction>,
    AbstractMutableEntityService<Classification, String> {

    companion object : MachineLearningService.BaseCompanionObject() {

        /**
         * Build a model for the given [io.openenterprise.incite.data.domain.Classification] if there is such an entity.
         *
         * @param id The [UUID] of [Classification] as [String]
         * @return The [UUID] of [Classification.Model]
         * @throws EntityNotFoundException If no such [Classification]
         */
        @JvmStatic
        @Throws(EntityNotFoundException::class)
        @QuerySqlFunction(alias = "build_classification_model")
        fun buildModel(id: String): UUID {
            val classificationService = getBean(ClassificationService::class.java)
            val classification = classificationService.retrieve(id)
                ?: throw EntityNotFoundException("Classification with ID, $id, is not found")
            val sparkModel: Model<*> = classificationService.buildModel(classification)

            return classificationService.persistModel(classification, sparkModel as MLWritable)
        }

        /**
         * Perform classification defined by the given [io.openenterprise.incite.data.domain.Classification] with the
         * latest [Classification.Model] if there is any and write the result to the given sinks defined in the given
         * [Classification].
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
    }
}