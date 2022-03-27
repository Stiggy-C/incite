package io.openenterprise.ignite.cache.query.ml

import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import java.util.*
import javax.inject.Named

@Named
class RecommendationFunction : AbstractFunction() {

    companion object : BaseCompanionObject() {

        @JvmStatic
        @QuerySqlFunction(alias = "build_als_model")
        fun buildAlsModel(
            sql: String,
            implicitPreference: Boolean = false,
            maxIteration: Int = 10,
            numberOfItemBlocks: Int = 10,
            numberOfUserBlocks: Int = 10,
            regularization: Double = 1.0
        ): UUID {
            val recommendationFunction = getBean(RecommendationFunction::class.java)
            val dataset = recommendationFunction.loadDatasetFromSql(sql)
            val alsModel = recommendationFunction.buildAlsModel(
                dataset,
                implicitPreference,
                maxIteration,
                numberOfItemBlocks,
                numberOfUserBlocks,
                regularization
            )

            return recommendationFunction.putToCache(alsModel)
        }

        @JvmStatic
        @QuerySqlFunction(alias = "als_predict")
        fun alsPredict(modelId: String, jsonOrSql: String, table: String, primaryKeyColumn: String): Long {
            val recommendationFunction = getBean(RecommendationFunction::class.java)
            val alsModel: ALSModel = recommendationFunction.getFromCache(UUID.fromString(modelId))

            val dataset = recommendationFunction.predict(alsModel, jsonOrSql)

            writeToTable(dataset, table, primaryKeyColumn, SaveMode.Append)

            return dataset.count()
        }

        @JvmStatic
        @QuerySqlFunction(alias = "als_recommend_for_all_users")
        fun alsRecommendForAllUsers(modelId: String, numberOfItems: Int, table: String, primaryKeyColumn: String): Long {
            val recommendationFunction = getBean(RecommendationFunction::class.java)
            val alsModel: ALSModel = recommendationFunction.getFromCache(UUID.fromString(modelId))
            val dataset = recommendationFunction.recommendForAllUsers(alsModel, numberOfItems)

            writeToTable(dataset, table, primaryKeyColumn, SaveMode.Append)

            return dataset.count()
        }

        @JvmStatic
        @QuerySqlFunction(alias = "als_recommend_for_users")
        fun alsRecommendForUsersSubset(
            modelId: String,
            jsonOrSql: String,
            numberOfItems: Int,
            table: String,
            primaryKeyColumn: String
        ): Long {
            val recommendationFunction = getBean(RecommendationFunction::class.java)
            val alsModel: ALSModel = recommendationFunction.getFromCache(UUID.fromString(modelId))
            val dataset = recommendationFunction.recommendForUsersSubset(alsModel, jsonOrSql, numberOfItems)

            writeToTable(dataset, table, primaryKeyColumn, SaveMode.Append)

            return dataset.count()
        }
    }

    fun buildAlsModel(
        dataset: Dataset<Row>,
        implicitPreference: Boolean = false,
        maxIteration: Int = 10,
        numberOfItemBlocks: Int = 10,
        numberOfUserBlocks: Int = 10,
        regularization: Double = 1.0
    ): ALSModel {
        val als = ALS()
        als.implicitPrefs = implicitPreference
        als.maxIter = maxIteration
        als.numItemBlocks = numberOfItemBlocks
        als.numUserBlocks = numberOfUserBlocks
        als.regParam = regularization

        return als.fit(dataset)
    }

    fun recommendForAllUsers(alsModel: ALSModel, numberOfItems: Int): Dataset<Row> =
        alsModel.recommendForAllUsers(numberOfItems)

    fun recommendForUsersSubset(alsModel: ALSModel, jsonOrSql: String, numberOfItems: Int): Dataset<Row> {
        val dataset = if (isJson(jsonOrSql)) {
            loadDatasetFromJson(jsonOrSql)
        } else {
            loadDatasetFromSql(jsonOrSql)
        }

        return alsModel.recommendForUserSubset(dataset, numberOfItems)
    }
}