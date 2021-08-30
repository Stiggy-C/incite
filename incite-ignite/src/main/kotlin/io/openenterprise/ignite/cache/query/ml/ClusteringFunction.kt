package io.openenterprise.ignite.cache.query.ml

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.springframework.context.ApplicationContextUtil
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.Model
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Value
import org.zeroturnaround.zip.ZipUtil
import java.io.File
import java.io.IOException
import java.util.*
import javax.cache.Cache
import javax.inject.Inject
import javax.inject.Named

@Named
open class ClusteringFunction {

    companion object {

        @JvmStatic
        @QuerySqlFunction(alias = "build_k_means_model")
        fun buildKMeanModel(sql: String, featuresColumns: String, k: Int, maxIteration: Int, seed: Long): String {
            val applicationContext = ApplicationContextUtil.getApplicationContext()!!
            val clusteringFunction = applicationContext.getBean(ClusteringFunction::class.java)
            val dataset = clusteringFunction.loadDataset(sql)
            val kMeansModel = clusteringFunction.buildKMeansModel(dataset, featuresColumns, k, maxIteration, seed)

            return clusteringFunction.putModelToCache(kMeansModel).toString()
        }

        @JvmStatic
        @QuerySqlFunction(alias = "k_means_predict")
        fun kMeansPredict(uuid: String, jsonOrSql: String): String {
            val clusteringFunction = getBean(ClusteringFunction::class.java)
            val objectMapper = getBean(ObjectMapper::class.java)

            var jsonNode: JsonNode? = null
            try {
                jsonNode = objectMapper.readTree(jsonOrSql)
            } catch (e: IOException) {
                // Given is not a JSON string assuming it is an SQL query for now
            }

            val dataset = if (jsonNode == null) {
                clusteringFunction.loadDataset(jsonOrSql)
            } else {
                val sparkSession = getBean(SparkSession::class.java)
                val tempJsonFilePath = "${FileUtils.getTempDirectoryPath()}/incite/ml/temp/${UUID.randomUUID()}.json"
                val tempJsonFile = File(tempJsonFilePath)

                objectMapper.writeValue(tempJsonFile, jsonNode)

                try {
                    sparkSession.read().json(tempJsonFilePath)
                } finally {
                    FileUtils.deleteQuietly(tempJsonFile)
                }
            }

            @Suppress("UNCHECKED_CAST")
            val kMeansModel = clusteringFunction.getModelFromCache(
                UUID.fromString(uuid),
                KMeansModel::class.java as Class<Model<KMeansModel>>
            ) as KMeansModel

            return clusteringFunction.kMeansPredict(dataset, kMeansModel)
        }

        @JvmStatic
        protected fun <T> getBean(clazz: Class<T>): T {
            val applicationContext = ApplicationContextUtil.getApplicationContext()!!
            return applicationContext.getBean(clazz)
        }
    }

    @Named("mlModelsCache")
    lateinit var mlModelsCache: Cache<UUID, File>

    @Value("\${ignite.sqlConfiguration.sqlSchemas}")
    lateinit var schemas: Array<String>

    @Inject
    lateinit var sparkSession: SparkSession

    fun buildKMeansModel(
        dataset: Dataset<Row>,
        featuresColumns: String,
        k: Int,
        maxIteration: Int,
        seed: Long
    ): KMeansModel {
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

    @Suppress("UNCHECKED_CAST")
    protected fun <T : Model<T>> getModelFromCache(uuid: UUID, clazz: Class<Model<T>>): Model<T> {
        val path = "${FileUtils.getTempDirectoryPath()}/incite/ml/$uuid"
        val directory = File(path)
        val zipFile = mlModelsCache.get(uuid)

        ZipUtil.unpack(zipFile, directory)

        return MethodUtils.invokeStaticMethod(clazz, "load", directory.path) as Model<T>
    }

    protected fun loadDataset(sql: String): Dataset<Row> {
        return sparkSession.read()
            .format("jdbc")
            .option("query", sql)
            .option("driver", "org.apache.ignite.IgniteJdbcThinDriver")
            .option("url", "jdbc:ignite:thin://localhost:10800/${schemas[0]}?lazy=true")
            .option("user", "ignite")
            .option("password", "ignite")
            .load()
    }

    protected fun putModelToCache(model: MLWritable): UUID {
        val uuid = UUID.randomUUID()
        val path = "${FileUtils.getTempDirectoryPath()}/incite/ml/$uuid"
        val directory = File(path)
        val zipFile = File("$path.zip")

        model.write().overwrite().save(path)
        ZipUtil.pack(directory, zipFile)

        mlModelsCache.put(uuid, zipFile)

        return uuid
    }
}