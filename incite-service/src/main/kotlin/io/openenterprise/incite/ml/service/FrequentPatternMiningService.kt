package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.FPGrowth
import io.openenterprise.incite.data.domain.FrequentPatternMining
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.IgniteException
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.fpm.FPGrowthModel
import org.apache.spark.ml.util.MLWritable
import java.util.*
import javax.persistence.EntityNotFoundException

interface FrequentPatternMiningService : MachineLearningService<FrequentPatternMining>,
    AbstractMutableEntityService<FrequentPatternMining, String> {

    companion object : MachineLearningService.BaseCompanionObject() {

        @JvmStatic
        @QuerySqlFunction(alias = "frequent_pattern_mining_predict")
        fun predict(id: String, jsonOrSql: String): Long {
            val frequentPatternMiningService = getBean(FrequentPatternMiningService::class.java)
            val frequentPatternMining = frequentPatternMiningService.retrieve(id)
                ?: throw EntityNotFoundException("FrequentPatternMining with ID, $id, is not found")
            val result = frequentPatternMiningService.predict(frequentPatternMining, jsonOrSql)

            writeToSinks(result, frequentPatternMining.sinks)

            return result.count()
        }

        @JvmStatic
        @QuerySqlFunction(alias = "set_up_frequent_pattern_mining")
        fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID {
            val algorithm = mergeParamsIntoAlgorithm(
                FrequentPatternMining.SupportedAlgorithm.valueOf(algo).clazz.getDeclaredConstructor()
                    .newInstance() as FrequentPatternMining.Algorithm, algoSpecificParams
            )
            val frequentPatternMining = setUpMachineLearning(
                FrequentPatternMiningService::class.java,
                FrequentPatternMining(),
                algorithm,
                sourceSql,
                sinkTable,
                primaryKeyColumns
            )

            getBean(FrequentPatternMiningService::class.java).create(frequentPatternMining)

            return UUID.fromString(frequentPatternMining.id)
        }

        @JvmStatic
        @Throws(IgniteException::class)
        @QuerySqlFunction(alias = "train_frequent_pattern_mining_model")
        fun train(id: String): UUID {
            val frequentPatternMiningService = getBean(FrequentPatternMiningService::class.java)
            val frequentPatternMining = frequentPatternMiningService.retrieve(id) ?: throw IgniteException(
                EntityNotFoundException("ClusterAnalysis (ID: $id) is not found")
            )
            val sparkModel = when (frequentPatternMining.algorithm) {
                is FPGrowth -> frequentPatternMiningService.train<FPGrowthModel>(frequentPatternMining)
                else -> throw IgniteException(UnsupportedOperationException())
            }

            return frequentPatternMiningService.persistModel(frequentPatternMining, sparkModel as MLWritable)
        }
    }
}