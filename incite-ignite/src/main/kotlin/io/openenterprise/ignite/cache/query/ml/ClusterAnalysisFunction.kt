package io.openenterprise.ignite.cache.query.ml

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.spark.sql.DatasetUtils
import io.openenterprise.springframework.context.ApplicationContextUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.FileWriterWithEncoding
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteJdbcThinDriver
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.ignite.configuration.ClientConnectorConfiguration
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.shared.HasFeaturesCol
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.zeroturnaround.zip.ZipUtil
import java.io.File
import java.io.IOException
import java.util.*
import javax.cache.Cache
import javax.inject.Inject
import javax.inject.Named

@Named
open class ClusterAnalysisFunction {

    companion object {

        /**
         * Build a [org.apache.spark.ml.clustering.BisectingKMeansModel] from given input.
         *
         * @return The [java.util.UUID] of the model
         */
        @JvmStatic
        @QuerySqlFunction(alias = "build_bisecting_k_means_model")
        fun buildBisectingKMeansModel(
            sql: String,
            featuresColumns: String,
            k: Int,
            maxIteration: Int,
            seed: Long
        ): UUID {
            val clusterAnalysisFunction = getFunctionInstance()
            val dataset = clusterAnalysisFunction.loadDataset(sql)
            val bisectingKMeansModel =
                clusterAnalysisFunction.buildBisectingKMeansModel(dataset, featuresColumns, k, maxIteration, seed)

            return clusterAnalysisFunction.putToCache(bisectingKMeansModel)
        }

        /**
         * Build a [org.apache.spark.ml.clustering.KMeansModel] from given input.
         *
         * @return The [java.util.UUID] of the model
         */
        @JvmStatic
        @QuerySqlFunction(alias = "build_k_means_model")
        fun buildKMeansModel(sql: String, featuresColumns: String, k: Int, maxIteration: Int, seed: Long): UUID {
            val clusterAnalysisFunction = getFunctionInstance()
            val dataset = clusterAnalysisFunction.loadDataset(sql)
            val kMeansModel = clusterAnalysisFunction.buildKMeansModel(dataset, featuresColumns, k, maxIteration, seed)

            return clusterAnalysisFunction.putToCache(kMeansModel)
        }

        /**
         * Perform BiseKMeans predict with given model and given json or SQL query.
         *
         * @return Result in JSON format
         */
        @JvmStatic
        @QuerySqlFunction(alias = "bisecting_k_means_predict")
        fun bisectingKMeansPredict(modelId: String, jsonOrSql: String): String {
            val clusterAnalysisFunction = getFunctionInstance()
            val bisectingKMeansModel: BisectingKMeansModel =
                clusterAnalysisFunction.getFromCache(UUID.fromString(modelId))
            val dataset = clusterAnalysisFunction.predict(jsonOrSql, bisectingKMeansModel)

            return DatasetUtils.toJson(dataset)
        }

        /**
         * Perform KMeans predict with given model and given json or SQL query.
         *
         * @return Result in JSON format
         */
        @JvmStatic
        @QuerySqlFunction(alias = "k_means_predict")
        fun kMeansPredict(modelId: String, jsonOrSql: String): String {
            val clusterAnalysisFunction = getFunctionInstance()
            val kMeansModel: KMeansModel = clusterAnalysisFunction.getFromCache(UUID.fromString(modelId))
            val dataset = clusterAnalysisFunction.predict(jsonOrSql, kMeansModel)

            return DatasetUtils.toJson(dataset)
        }

        protected fun getFunctionInstance(): ClusterAnalysisFunction =
            ApplicationContextUtils.getApplicationContext()!!.getBean(ClusterAnalysisFunction::class.java)
    }

    @Inject
    protected lateinit var ignite: Ignite

    @Inject
    @Named("mlModelsCache")
    protected lateinit var modelsCache: Cache<UUID, File>

    @Inject
    protected lateinit var objectMapper: ObjectMapper

    @Inject
    protected lateinit var sparkSession: SparkSession

    fun buildBisectingKMeansModel(
        dataset: Dataset<Row>,
        featuresColumns: String,
        k: Int,
        maxIteration: Int,
        seed: Long
    ): BisectingKMeansModel {
        val bisectingKMeans = BisectingKMeans()
        bisectingKMeans.k = k
        bisectingKMeans.maxIter = maxIteration
        bisectingKMeans.seed = seed

        return buildModel(bisectingKMeans, dataset, StringUtils.split(featuresColumns, ","))
    }

    fun buildKMeansModel(
        dataset: Dataset<Row>,
        featuresColumns: String,
        k: Int,
        maxIteration: Int,
        seed: Long
    ): KMeansModel {
        val kMeans = KMeans()
        kMeans.k = k
        kMeans.maxIter = maxIteration
        kMeans.seed = seed

        return buildModel(kMeans, dataset, StringUtils.split(featuresColumns, ","))
    }

    @Suppress("UNCHECKED_CAST")
    fun <M : Model<M>> getFromCache(modelId: UUID): M {
        val path = "${FileUtils.getTempDirectoryPath()}/incite/ml/$modelId"
        val directory = File(path)
        val zipFile = modelsCache.get(modelId)

        ZipUtil.unpack(zipFile, directory)

        return MethodUtils.invokeStaticMethod(Model::class.java, "load", directory.path) as M
    }

    fun <M : Model<M>> predict(jsonOrSql: String, model: Model<M>): Dataset<Row> {
        var jsonNode: JsonNode? = null
        var tempJsonFile: File? = null
        try {
            jsonNode = objectMapper.readTree(jsonOrSql)
        } catch (e: IOException) {
            // Given is not a JSON string. Let's assume it is an SQL query for now.
        }

        val dataset = if (jsonNode == null) {
            loadDataset(jsonOrSql)
        } else {
            val tempJsonFilePath = "${FileUtils.getTempDirectoryPath()}/incite/ml/temp/${UUID.randomUUID()}.json"
            tempJsonFile = File(tempJsonFilePath)

            val fileWriter = FileWriterWithEncoding(tempJsonFile, Charsets.UTF_8, false)
            IOUtils.write(jsonOrSql, fileWriter)

            sparkSession.read().json(tempJsonFilePath)
        }

        try {
            return model.transform(dataset)
        } finally {
            FileUtils.deleteQuietly(tempJsonFile)
        }
    }

    fun <T : MLWritable> putToCache(model: T): UUID {
        val modelId = UUID.randomUUID()
        val path = "${FileUtils.getTempDirectoryPath()}/incite/ml/$modelId"
        val directory = File(path)
        val zipFile = File("$path.zip")

        model.write().overwrite().save(path)
        ZipUtil.pack(directory, zipFile)

        modelsCache.put(modelId, zipFile)

        return modelId
    }

    protected fun loadDataset(sql: String): Dataset<Row> {
        val igniteConfiguration = ignite.configuration()
        val clientConnectorConfiguration = igniteConfiguration.clientConnectorConfiguration
        val clientConnectorPort =
            if (Objects.isNull(clientConnectorConfiguration)) ClientConnectorConfiguration.DFLT_PORT else clientConnectorConfiguration!!.port
        val sqlConfiguration = igniteConfiguration.sqlConfiguration
        val sqlSchema = if (sqlConfiguration.sqlSchemas.isEmpty()) "incite" else sqlConfiguration.sqlSchemas[0]

        return DatasetUtils.load(
            sparkSession,
            sql,
            IgniteJdbcThinDriver::class.java.name,
            "jdbc:ignite:thin://localhost:${clientConnectorPort}/${sqlSchema}?lazy=true",
            "ignite",
            "ignite"
        )
    }

    private fun <A : Estimator<M>, M : Model<M>> buildModel(
        algorithm: A,
        dataset: Dataset<Row>,
        featuresColumns: Array<String>
    ): M {
        assert(algorithm is HasFeaturesCol)

        @Suppress("unchecked_cast")
        val transformedDataset =
            VectorAssembler().setInputCols(featuresColumns).setOutputCol(((algorithm as HasFeaturesCol).featuresCol))
                .transform(dataset)

        return algorithm.fit(transformedDataset)
    }
}