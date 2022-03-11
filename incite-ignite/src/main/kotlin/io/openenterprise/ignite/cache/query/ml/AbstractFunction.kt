package io.openenterprise.ignite.cache.query.ml

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.ignite.spark.IgniteDataFrameConstants
import io.openenterprise.spark.sql.DatasetUtils
import io.openenterprise.springframework.context.ApplicationContextUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.FileWriterWithEncoding
import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteJdbcThinDriver
import org.apache.ignite.configuration.ClientConnectorConfiguration
import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.zeroturnaround.zip.ZipUtil
import java.io.File
import java.io.IOException
import java.util.*
import javax.cache.Cache
import javax.inject.Inject
import javax.inject.Named

abstract class AbstractFunction {

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

    abstract class BaseCompanionObject {

        protected fun <T> getBean(clazz: Class<T>): T  =
            ApplicationContextUtils.getApplicationContext()!!.getBean(clazz)

        protected fun writeToTable(dataset: Dataset<Row>, table: String, primaryKeyColumn: String, saveMode: SaveMode) {
            val dataFrameWriter = dataset.write()
                .format(IgniteDataFrameConstants.FORMAT)
                .mode(saveMode)
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(),primaryKeyColumn)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), table)


            dataFrameWriter.save()
        }
    }
}