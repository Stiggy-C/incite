package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.FrequentPatternMining
import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.IgniteException
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import java.util.*

interface FrequentPatternMiningService :
    MachineLearningService<FrequentPatternMining, FrequentPatternMining.Algorithm, FrequentPatternMining.Model>,
    AbstractMutableEntityService<FrequentPatternMining, String> {

    companion object :
        MachineLearningService.BaseCompanionObject<FrequentPatternMining, FrequentPatternMining.Algorithm, FrequentPatternMining.Model, FrequentPatternMiningService>() {

        @JvmStatic
        @QuerySqlFunction(alias = "frequent_pattern_mining_predict")
        override fun predict(id: String, jsonOrSql: String): Long = super.predict(id, jsonOrSql)

        @JvmStatic
        @QuerySqlFunction(alias = "set_up_frequent_pattern_mining")
        override fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID = super.setUp(algo, algoSpecificParams, sourceSql, sinkTable, primaryKeyColumns)

        @JvmStatic
        @Throws(IgniteException::class)
        @QuerySqlFunction(alias = "train_frequent_pattern_mining_model")
        override fun train(id: String): UUID = super.train(id)
        override fun getMachineLearningClass(): Class<FrequentPatternMining> = FrequentPatternMining::class.java

        override fun getMachineLearningAlgorithmClass(algo: String): Class<out MachineLearning.Algorithm> =
            FrequentPatternMining.SupportedAlgorithm.valueOf(algo).clazz

        override fun getMachineLearningService(): FrequentPatternMiningService =
            getBean(FrequentPatternMiningService::class.java)
    }
}