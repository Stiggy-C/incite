package io.openenterprise.incite.ml.service

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3control.model.NotFoundException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import io.awspring.cloud.core.io.s3.SimpleStorageResource
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.incite.service.PipelineServiceImpl
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.commons.collections4.IteratorUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.ArrayUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteJdbcThinDriver
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration
import org.apache.ignite.configuration.ClientConnectorConfiguration
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.task.SyncTaskExecutor
import org.springframework.transaction.support.TransactionTemplate
import java.io.File
import java.io.IOException
import java.util.*
import java.util.stream.Collectors
import javax.cache.Cache
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.EntityNotFoundException

abstract class AbstractMachineLearningServiceImpl<T : MachineLearning<A, M>, M : MachineLearning.Model<M>, A : MachineLearning.Algorithm>(
    private val datasetService: DatasetService,
    private val pipelineService: PipelineService
) : AbstractAbstractMutableEntityServiceImpl<T, String>(), MachineLearningService<T, A, M> {

    @Inject
    protected lateinit var amazonS3: AmazonS3

    @Inject
    protected lateinit var ignite: Ignite

    /*@Inject
    @Named("mlModelsCache")
    protected lateinit var modelsCache: Cache<UUID, File>*/

    @Inject
    protected lateinit var objectMapper: ObjectMapper

    @Value("\${incite.aws.s3.bucket:incite}")
    protected lateinit var s3Bucket: String

    @Inject
    protected lateinit var sparkSession: SparkSession

    @Inject
    protected lateinit var transactionTemplate: TransactionTemplate

    @Suppress("UNCHECKED_CAST")
    override fun <M : Model<M>> getFromS3(modelId: UUID, clazz: Class<M>): M {
        if (!amazonS3.doesBucketExistV2(s3Bucket)) {
            throw NotFoundException("Bucket, $s3Bucket, is not exist")
        }

        val s3path = "ml/models/$modelId"
        val s3Resource = SimpleStorageResource(amazonS3, s3Bucket, s3path, SyncTaskExecutor())
        val s3Uri = s3Resource.s3Uri

        return MethodUtils.invokeStaticMethod(
            clazz, "load",
            StringUtils.replace(s3Uri.toString(), "s3", "s3a")
        ) as M
    }

    override fun predict(entity: T, jsonOrSql: String): Dataset<Row> {
        if (entity.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        assert(pipelineService is PipelineServiceImpl)

        val sparkModel: Model<*> = getSparkModel(entity)
        val dataset = postProcessLoadedDataset(entity.algorithm, sparkModel, loadDataset(jsonOrSql))
        val result = predict(sparkModel, dataset)

        datasetService.write(result, entity.sinks, false)

        return result
    }

    override fun persistModel(entity: T, sparkModel: MLWritable): UUID {
        val modelId = putToS3(sparkModel)
        val model = entity.newModelInstance()
        model.id = modelId.toString()

        entity.models.add(model)

        transactionTemplate.execute {
            update(entity)
        }

        return modelId
    }

    override fun putToS3(model: MLWritable): UUID {
        if (!amazonS3.doesBucketExistV2(s3Bucket)) {
            amazonS3.createBucket(s3Bucket)
        }

        val modelId = UUID.randomUUID()

        val s3path = "ml/models/$modelId"
        val s3Resource = SimpleStorageResource(amazonS3, s3Bucket, s3path, SyncTaskExecutor())
        val s3Uri = s3Resource.s3Uri

        model.write().overwrite().save(StringUtils.replace(s3Uri.toString(), "s3", "s3a"))

        return modelId
    }

    override fun <SM : Model<SM>> train(entity: T): SM {
        val dataset = getAggregatedDataset(entity)

        return buildSparkModel(entity, dataset)
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

    protected abstract fun <M : Model<M>> buildSparkModel(entity: T, dataset: Dataset<Row>): M

    protected abstract fun getSparkModel(algorithm: MachineLearning.Algorithm, modelId: String): Model<*>

    protected fun isJson(string: String): Boolean {
        try {
            objectMapper.readTree(string)
        } catch (e: IOException) {
            return false
        }

        return true
    }

    protected fun loadDataset(jsonOrSql: String): Dataset<Row> = if (isJson(jsonOrSql)) {
        loadDatasetFromJson(jsonOrSql)
    } else {
        loadDatasetFromSql(jsonOrSql)
    }

    protected fun loadDatasetFromJson(jsonString: String): Dataset<Row> {
        val jsonNode = objectMapper.readTree(jsonString)
        val jsonData = if (jsonNode.isArray) {
            IteratorUtils.toList((jsonNode as ArrayNode).elements())
        } else {
            listOf(jsonNode)
        }

        return sparkSession.read().json(
            sparkSession.createDataset(
                jsonData.stream().map { it.toString() }.collect(Collectors.toList()),
                Encoders.STRING()
            )
        )
    }

    protected fun loadDatasetFromSql(sql: String): Dataset<Row> {
        val jdbcSource = JdbcSource()
        jdbcSource.rdbmsDatabase = buildEmbeddedIgniteRdbmsDatabase()
        jdbcSource.query = sql

        return datasetService.load(jdbcSource)
    }

    protected open fun <A : MachineLearning.Algorithm, M : Model<*>> postProcessLoadedDataset(
        algorithm: A,
        model: M,
        dataset: Dataset<Row>
    ): Dataset<Row> = dataset

    protected open fun <A : MachineLearning.Algorithm> postProcessLoadedDataset(
        algorithm: A,
        dataset: Dataset<Row>
    ): Dataset<Row> = dataset

    protected open fun <M : Model<M>> postProcessLoadedDataset(model: Model<M>, dataset: Dataset<Row>): Dataset<Row> =
        dataset

    protected fun <M : Model<M>> predict(model: Model<M>, dataset: Dataset<Row>): Dataset<Row> =
        model.transform(dataset)

    private fun getSparkModel(entity: T): Model<*> =
        getSparkModel(
            entity.algorithm,
            entity.models.stream().findFirst().orElseThrow { EntityNotFoundException() }.id
                ?: throw IllegalStateException()
        )
}