package io.openenterprise.ignite.cache.query.ml

import io.openenterprise.spark.sql.DatasetUtils
import org.apache.commons.lang3.StringUtils
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.ml.classification.Classifier
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.shared.HasFeaturesCol
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*
import javax.inject.Named

@Named
class ClassificationFunction : AbstractFunction() {

    companion object : BaseCompanionObject() {

        @JvmStatic
        @QuerySqlFunction(alias = "build_logistic_regression_model")
        fun buildLogisticRegressionModel(
            sql: String,
            family: String,
            featureColumns: String,
            labelColumn: String,
            elasticNetMixing: Double,
            maxIteration: Int,
            regularization: Double
        ): UUID {
            val classificationFunction = getBean(ClassificationFunction::class.java)
            val dataset = classificationFunction.loadDatasetFromSql(sql)
            val logisticRegressionModel = classificationFunction.buildLogisticRegressionModel(
                dataset,
                family,
                featureColumns,
                labelColumn,
                elasticNetMixing,
                maxIteration,
                regularization
            )

            return classificationFunction.putToCache(logisticRegressionModel)
        }

        @JvmStatic
        @QuerySqlFunction(alias = "logistic_regression_predict")
        fun logisticRegressionPredict(modelId: String, jsonOrSql: String): String {
            val classificationFunction = getBean(ClassificationFunction::class.java)
            val logisticRegressionModel: LogisticRegressionModel =
                classificationFunction.getFromCache(UUID.fromString(modelId))
            val dataset = classificationFunction.predict(logisticRegressionModel, jsonOrSql)

            return DatasetUtils.toJson(dataset)
        }
    }

    fun buildLogisticRegressionModel(
        dataset: Dataset<Row>,
        family: String?,
        featuresColumns: String,
        labelColumn: String,
        elasticNetMixing: Double,
        maxIteration: Int,
        regularization: Double
    ): LogisticRegressionModel {
        val logisticRegression = LogisticRegression()
        if (family != null) {
            logisticRegression.family = family
        }
        logisticRegression.elasticNetParam = elasticNetMixing
        logisticRegression.maxIter = maxIteration
        logisticRegression.regParam = regularization

        return buildModel(logisticRegression, dataset, labelColumn, StringUtils.split(featuresColumns, ","))
    }

    private fun <A : Classifier<Vector, *, M>, M : ClassificationModel<Vector, M>> buildModel(
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