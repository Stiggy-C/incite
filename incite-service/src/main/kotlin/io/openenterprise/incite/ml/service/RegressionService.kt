package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.data.domain.Regression
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.IgniteException
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import java.util.*

interface RegressionService : MachineLearningService<Regression, Regression.Algorithm, Regression.Model>,
    AbstractMutableEntityService<Regression, String> {

    companion object :
        MachineLearningService.BaseCompanionObject<Regression, Regression.Algorithm, Regression.Model, RegressionService>() {

        @JvmStatic
        @QuerySqlFunction(alias = "regression_predict")
        override fun predict(id: String, jsonOrSql: String): Long {
            return super.predict(id, jsonOrSql)
        }

        @JvmStatic
        @QuerySqlFunction(alias = "set_up_regression")
        override fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID {
            return super.setUp(algo, algoSpecificParams, sourceSql, sinkTable, primaryKeyColumns)
        }

        @JvmStatic
        @Throws(IgniteException::class)
        @QuerySqlFunction(alias = "train_regression_model")
        override fun train(id: String): UUID {
            return super.train(id)
        }

        override fun getMachineLearningClass(): Class<Regression> = Regression::class.java

        override fun getMachineLearningAlgorithmClass(algo: String): Class<out MachineLearning.Algorithm> =
            Regression.SupportedAlgorithm.valueOf(algo).clazz

        override fun getMachineLearningService(): RegressionService = getBean(RegressionService::class.java)
    }
}