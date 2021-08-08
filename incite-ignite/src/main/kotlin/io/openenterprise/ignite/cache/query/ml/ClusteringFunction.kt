package io.openenterprise.ignite.cache.query.ml

import io.openenterprise.springframework.context.ApplicationContextUtil
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import javax.inject.Inject
import javax.inject.Named

@Named
open class ClusteringFunction {

    companion object {

        @JvmStatic
        @QuerySqlFunction(alias = "kmean_predict")
        fun kMeanPredict(sql: String, featuresColumns: String, k: Int, maxIteration: Int, seed: Long): String {
            val applicationContext = ApplicationContextUtil.getApplicationContext()!!
            val clusteringFunction = applicationContext.getBean(ClusteringFunction::class.java)
            val dataset = clusteringFunction.executeQuery(sql)
            val kMeansModel = clusteringFunction.buildKMeanModel(dataset, featuresColumns, k, maxIteration, seed)

            return clusteringFunction.kMeansPredict(dataset, kMeansModel)
        }
    }

    @Inject
    lateinit var sparkSession: SparkSession

    fun buildKMeanModel(dataset: Dataset<Row>, featuresColumns: String, k: Int, maxIteration: Int, seed: Long): KMeansModel {
        val kMeans = KMeans()
        kMeans.featuresCol = featuresColumns
        kMeans.k = k
        kMeans.maxIter = maxIteration
        kMeans.seed = seed

        return kMeans.fit(dataset)
    }

    fun kMeansPredict(dataset: Dataset<Row>, kMeansModel: KMeansModel): String {
        val transformedDataset = kMeansModel.transform(dataset)

        return transformedDataset.schema().json()
    }

    protected fun executeQuery(sql: String): Dataset<Row> {
        return sparkSession.sql(sql)
    }
}