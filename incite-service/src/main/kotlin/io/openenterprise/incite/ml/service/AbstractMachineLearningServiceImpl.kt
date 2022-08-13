package io.openenterprise.incite.ml.service

import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.incite.data.domain.FrequentPatternMining
import io.openenterprise.incite.data.domain.JdbcSource
import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.data.domain.RdbmsDatabase
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.incite.service.PipelineServiceImpl
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.FileWriterWithEncoding
import org.apache.commons.lang3.ArrayUtils
import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteJdbcThinDriver
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration
import org.apache.ignite.configuration.ClientConnectorConfiguration
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration
import org.apache.spark.ml.Model
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

abstract class AbstractMachineLearningServiceImpl<T : MachineLearning<*, *>>(
    private val datasetService: DatasetService,
    private val pipelineService: PipelineService

) :
    MachineLearningService<T>,
    AbstractAbstractMutableEntityServiceImpl<T, String>() {

    @Inject
    protected lateinit var ignite: Ignite

    @Inject
    @Named("mlModelsCache")
    protected lateinit var modelsCache: Cache<UUID, File>

    @Inject
    protected lateinit var objectMapper: ObjectMapper

    @Inject
    protected lateinit var sparkSession: SparkSession

    @Suppress("UNCHECKED_CAST")
    override fun <M : Model<M>> getFromCache(modelId: UUID): M {
        val path = "${FileUtils.getTempDirectoryPath()}/incite/ml/$modelId"
        val directory = File(path)
        val zipFile = modelsCache.get(modelId)

        ZipUtil.unpack(zipFile, directory)

        return MethodUtils.invokeStaticMethod(Model::class.java, "load", directory.path) as M
    }

    override fun putToCache(model: MLWritable): UUID {
        val modelId = UUID.randomUUID()
        val path = "${FileUtils.getTempDirectoryPath()}/incite/ml/$modelId"
        val directory = File(path)
        val zipFile = File("$path.zip")

        model.write().overwrite().save(path)
        ZipUtil.pack(directory, zipFile)

        modelsCache.put(modelId, zipFile)

        return modelId
    }

    internal fun buildEmbeddedIgniteRdbmsDatabase(): RdbmsDatabase {
        val igniteConfiguration = ignite.configuration()
        val clientConnectorConfiguration = igniteConfiguration.clientConnectorConfiguration
        val clientConnectorPort =
            if (Objects.isNull(clientConnectorConfiguration))
                ClientConnectorConfiguration.DFLT_PORT
            else
                clientConnectorConfiguration!!.port
        val sqlConfiguration = igniteConfiguration.sqlConfiguration
        val sqlSchema =
            if (ArrayUtils.isEmpty(sqlConfiguration.sqlSchemas)) "incite" else sqlConfiguration.sqlSchemas[0]
        val defaultQueryEngine = if (ArrayUtils.isEmpty(sqlConfiguration.queryEnginesConfiguration))
            "h2"
        else {
            Arrays.stream(sqlConfiguration.queryEnginesConfiguration)
                .filter { it.isDefault }
                .map {
                    when (it) {
                        is CalciteQueryEngineConfiguration -> "calcite"
                        is IndexingQueryEngineConfiguration -> "h2"
                        else -> throw UnsupportedOperationException()
                    }
                }
                .findFirst()
                .get()
        }

        val rdbmsDatabase = RdbmsDatabase()
        rdbmsDatabase.driverClass = IgniteJdbcThinDriver::class.java.name
        rdbmsDatabase.password = "ignite"
        rdbmsDatabase.url =
            "jdbc:ignite:thin://localhost:${clientConnectorPort}/${sqlSchema}?lazy=true&queryEngine=${defaultQueryEngine}"
        rdbmsDatabase.username = "ignite"
        return rdbmsDatabase
    }

    protected fun getAggregatedDataset(entity: T): Dataset<Row> {
        val datasets = datasetService.load(entity.sources, Collections.emptyMap<String, Any>())

        return (pipelineService as PipelineServiceImpl).aggregate(datasets, entity.joins)
    }

    protected fun isJson(string: String): Boolean {
        try {
            objectMapper.readTree(string)
        } catch (e: IOException) {
            return false
        }

        return true
    }

    /**
     * TODO Refactor to take advantage of DatasetService
     */
    protected fun loadDatasetFromJson(json: String): Dataset<Row> {
        val tempJsonFilePath = "${FileUtils.getTempDirectoryPath()}/incite/ml/temp/${UUID.randomUUID()}.json"
        val tempJsonFile = File(tempJsonFilePath)

        val fileWriter = FileWriterWithEncoding(tempJsonFile, Charsets.UTF_8, false)
        IOUtils.write(json, fileWriter)

        try {
            return sparkSession.read().json(tempJsonFilePath)
        } finally {
            FileUtils.deleteQuietly(tempJsonFile)
        }
    }

    protected fun loadDatasetFromSql(sql: String): Dataset<Row> {
        val jdbcSource = JdbcSource()
        jdbcSource.rdbmsDatabase = buildEmbeddedIgniteRdbmsDatabase()
        jdbcSource.query = sql

        return datasetService.load(jdbcSource)
    }

    protected open fun postProcessLoadedDataset(
        algorithm: FrequentPatternMining.Algorithm,
        dataset: Dataset<Row>
    ): Dataset<Row> =
        dataset

    protected open fun <M : Model<M>> postProcessLoadedDataset(model: Model<M>, dataset: Dataset<Row>): Dataset<Row> =
        dataset

    protected fun <M : Model<M>> predict(model: Model<M>, jsonOrSql: String): Dataset<Row> {
        val dataset = if (isJson(jsonOrSql)) {
            loadDatasetFromJson(jsonOrSql)
        } else {
            loadDatasetFromSql(jsonOrSql)
        }

        return model.transform(postProcessLoadedDataset(model, dataset))
    }
}