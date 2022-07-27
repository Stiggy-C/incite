package io.openenterprise.incite.spark.sql.service

import com.google.common.base.CaseFormat
import io.openenterprise.ignite.spark.IgniteJdbcConstants
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.spark.sql.DatasetNonStreamingWriter
import io.openenterprise.incite.spark.sql.DatasetWriter
import io.openenterprise.incite.spark.sql.streaming.DatasetStreamingWriter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.api.java.function.VoidFunction2
import org.apache.spark.sql.*
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.expression.spel.support.StandardEvaluationContext
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject
import javax.inject.Named

@Named
open class DatasetServiceImpl(
    @Inject private val coroutineScope: CoroutineScope,
    @Value("\${io.openenterprise.incite.spark.checkpoint-location-root:./spark-checkpoints}") private val sparkCheckpointLocation: String,
    @Inject private val sparkSession: SparkSession,
    @Inject private val spelExpressionParser: SpelExpressionParser
) : DatasetService {

    companion object {

        private val LOG = LoggerFactory.getLogger(DatasetServiceImpl::class.java)
    }

    override fun load(source: Source): Dataset<Row> = load(source, Collections.emptyMap<String, Any>())

    override fun load(source: Source, variables: Map<String, *>): Dataset<Row> {
        @Suppress("NAME_SHADOWING")
        val source = when (source) {
            is JdbcSource -> manipulateSqlQuery(source, variables)
            else -> source
        }

        return when (source) {
            is FileSource -> load(source)
            is KafkaSource -> load(source)
            is JdbcSource -> load(source)
            else -> throw UnsupportedOperationException()
        }
    }

    override fun load(sources: List<Source>): List<Dataset<Row>> = load(sources, Collections.emptyMap<String, Any>())

    override fun load(sources: List<Source>, variables: Map<String, *>): List<Dataset<Row>> = sources.stream()
        .map {
            load(it, variables)
        }
        .collect(
            Collectors.toList()
        )

    override fun write(
        dataset: Dataset<Row>,
        sink: StreamingSink
    ): DatasetStreamingWriter {
        val trigger = when (sink.triggerType) {
            StreamingSink.TriggerType.Continuous -> {
                Trigger.Continuous(sink.triggerInterval)
            }
            StreamingSink.TriggerType.Once -> {
                Trigger.Once()
            }
            StreamingSink.TriggerType.ProcessingTime -> {
                Trigger.ProcessingTime(sink.triggerInterval)
            }
            else -> {
                throw UnsupportedOperationException()
            }
        }

        var dataStreamWriter: DataStreamWriter<*> = when (sink) {
            is FileSink -> {
                dataset.writeStream()
                    .format(sink.format.name.lowercase())
                    .option("path", sink.path)
            }
            is KafkaSink -> {
                // As of Dec 11, 2021, only support JSON for now
                dataset.toJSON()
                    .writeStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", sink.kafkaCluster.servers)
                    .option("topic", sink.topic)
            }
            is StreamingWrapper -> {
                dataset
                    .writeStream()
                    .foreachBatch(
                        VoidFunction2<Dataset<Row>, Long> { batch, batchId ->
                            LOG.info("About to write dataset of batch {} to non-streaming sink, {}", batchId, sink)

                            write(batch, sink.nonStreamingSink)
                        }
                    )
            }
            else -> // As of Dec 11, 2021, only support JSON for now
            {
                throw UnsupportedOperationException()
            }
        }

        val outputMode = when(sink.outputMode) {
            StreamingSink.OutputMode.APPEND -> OutputMode.Append()
            StreamingSink.OutputMode.COMPLETE -> OutputMode.Complete()
            StreamingSink.OutputMode.UPDATE -> OutputMode.Update()
        }

        dataStreamWriter = dataStreamWriter
            .option("checkpointLocation", "$sparkCheckpointLocation/${sink.id}")
            .outputMode(outputMode)
            .trigger(trigger)

        val datasetStreamWriter = DatasetStreamingWriter(dataStreamWriter, dataStreamWriter.start())

        coroutineScope.launch {
            datasetStreamWriter.streamingQuery.awaitTermination()
        }

        return datasetStreamWriter
    }

    /**
     * TODO Handle options for each type of sinks
     */
    override fun write(dataset: Dataset<Row>, sink: NonStreamingSink): DatasetNonStreamingWriter {
        var dataFrameWriter = when (sink) {
            is IgniteSink -> {
                dataset.write()
                    .format(IgniteJdbcConstants.FORMAT)
                    .option(IgniteJdbcConstants.PRIMARY_KEY_COLUMNS, sink.primaryKeyColumns)
            }
            is JdbcSink -> {
                dataset.write()
                    .format("jdbc")
            }
            else -> {
                throw UnsupportedOperationException()
            }
        }

        dataFrameWriter = when (sink) {
            is JdbcSink -> {
                dataFrameWriter = dataFrameWriter
                    .option("user", sink.rdbmsDatabase.username)
                    .option("password", sink.rdbmsDatabase.password)
                    .option(JDBCOptions.JDBC_DRIVER_CLASS(), sink.rdbmsDatabase.driverClass)
                    .option(JDBCOptions.JDBC_TABLE_NAME(), sink.table)
                    .option(JDBCOptions.JDBC_URL(), sink.rdbmsDatabase.url)

                if (StringUtils.isNotEmpty(sink.createTableColumnTypes)) {
                    dataFrameWriter = dataFrameWriter
                        .option(JDBCOptions.JDBC_CREATE_TABLE_COLUMN_TYPES(), sink.createTableColumnTypes)
                }

                if (StringUtils.isNotEmpty(sink.createTableOptions)) {
                    dataFrameWriter = dataFrameWriter
                        .option(JDBCOptions.JDBC_CREATE_TABLE_OPTIONS(), sink.createTableOptions)
                }

                dataFrameWriter
            }
            else -> {
                throw UnsupportedOperationException()
            }
        }

        val saveMode = org.apache.spark.sql.SaveMode.valueOf(
            CaseFormat.UPPER_UNDERSCORE.to(
                CaseFormat.UPPER_CAMEL,
                sink.saveMode.name
            )
        )

        dataFrameWriter = dataFrameWriter.mode(saveMode)

        dataFrameWriter.save()

        return DatasetNonStreamingWriter(dataFrameWriter)
    }

    override fun write(dataset: Dataset<Row>, sinks: List<Sink>, forceStreaming: Boolean): Set<DatasetWriter<*>> {
        @Suppress("NAME_SHADOWING")
        val sinks = if (forceStreaming) {
            sinks.stream()
                .map { (if (it is NonStreamingSink) StreamingWrapper(it) else it) as StreamingSink }
                .collect(Collectors.toList())
        } else {
            sinks.stream().peek {
                if (it is StreamingSink) {
                    it.streamingWrite = false
                }
            }.collect(Collectors.toList())
        }

        val writers = sinks.stream()
            .map {
                when (it) {
                    is NonStreamingSink -> write(dataset, it)
                    is StreamingSink -> write(dataset, it)
                    else -> throw UnsupportedOperationException()
                }
            }
            .collect(Collectors.toSet())

        return writers
    }

    private fun load(fileSource: FileSource): Dataset<Row> {
        val dataset: Dataset<Row> = if (fileSource.streamingRead) sparkSession.readStream()
            .format(fileSource.format.name.lowercase())
            .option("path", fileSource.path)
            .option("maxFilesPerTrigger", fileSource.maxFilesPerTrigger.toLong())
            .option("latestFirst", fileSource.latestFirst)
            .option("maxFileAge", fileSource.maxFileAge)
            .option("cleanSource", fileSource.cleanSource.name.lowercase())
            .option("sourceArchiveDirectory", fileSource.sourceArchiveDirectory)
            .load()
        else sparkSession.read()
            .format(fileSource.format.name.lowercase())
            .option("path", fileSource.path)
            .option("maxFilesPerTrigger", fileSource.maxFilesPerTrigger.toLong())
            .option("latestFirst", fileSource.latestFirst)
            .option("maxFileAge", fileSource.maxFileAge)
            .option("cleanSource", fileSource.cleanSource.name.lowercase())
            .option("sourceArchiveDirectory", fileSource.sourceArchiveDirectory)
            .load()

        return postLoad(fileSource, dataset)
    }

    /**
     * Load a [Dataset] from a [JdbcSource].
     *
     * @return [Dataset]
     */
    private fun load(jdbcSource: JdbcSource): Dataset<Row> {
        val dataset: Dataset<Row> = sparkSession.read()
            .format("jdbc")
            .option("query", jdbcSource.query)
            .option("driver", jdbcSource.rdbmsDatabase.driverClass)
            .option("url", jdbcSource.rdbmsDatabase.url)
            .option("user", jdbcSource.rdbmsDatabase.username)
            .option("password", jdbcSource.rdbmsDatabase.password)
            .load()

        return postLoad(jdbcSource, dataset)
    }

    /**
     * Load a [Dataset] from a [KafkaSource]. As of Dec 11, 2021, only support JSON message for now.
     *
     * @return [Dataset]
     */
    private fun load(kafkaSource: KafkaSource): Dataset<Row> {
        var dataset: Dataset<Row> = if (kafkaSource.streamingRead) sparkSession.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaSource.kafkaCluster.servers)
            .option("startingOffsets", kafkaSource.startingOffset.name.lowercase())
            .option("subscribe", kafkaSource.topic)
            .load()
        else sparkSession.read().format("kafka")
            .option("kafka.bootstrap.servers", kafkaSource.kafkaCluster.servers)
            .option("startingOffsets", "earliest")
            .option("subscribe", kafkaSource.topic)
            .load()

        dataset = dataset.selectExpr("cast(value as STRING)")

        return postLoad(kafkaSource, dataset)
    }

    private fun manipulateSqlQuery(jdbcSource: JdbcSource, variables: Map<String, *>): JdbcSource {
        val source = jdbcSource.clone() as JdbcSource

        val evaluationContext = StandardEvaluationContext()
        evaluationContext.setVariables(variables)

        var query: String = source.query
        val tokens = StringUtils.split(query, " ")
        val expressionTokens = Arrays.stream(tokens)
            .filter { token: String? -> StringUtils.startsWith(token, "#") }
            .collect(Collectors.toSet())

        for (token in expressionTokens) {
            val expression = spelExpressionParser.parseExpression(token)
            val value = expression.getValue(evaluationContext)
            val valueAsSqlString = if (Objects.isNull(value)) "null" else "'$value'"

            query = StringUtils.replace(query, token, valueAsSqlString)
        }

        source.query = query

        return source
    }

    private fun postLoad(source: Source, dataset: Dataset<Row>): Dataset<Row> {
        val interimDataset = if (CollectionUtils.isEmpty(source.fields))
            dataset
        else {
            val selects: Array<String> = source.fields!!.stream()
                .map { field ->
                    var selectClause = when (source) {
                        is KafkaSource -> "get_json_object(value, '$." + field.name + "')"
                        else -> field.name
                    }

                    if (StringUtils.isNotEmpty(field.function)) {
                        val evaluationContext = StandardEvaluationContext()
                        evaluationContext.setVariable("field", selectClause)

                        val functionTokens = field.function!!.split("#field").stream()
                            .map { token -> "\"" + token + "\"" }
                            .collect(Collectors.toList())

                        for (i in 0 until functionTokens.size) {
                            var token = functionTokens[i]
                            token = when (i) {
                                0 -> "$token + "
                                functionTokens.size - 1 -> " + $token"
                                else -> " + $token + "
                            }

                            functionTokens[i] = token
                        }

                        selectClause = spelExpressionParser
                            .parseExpression(java.lang.String.join("#field", functionTokens))
                            .getValue(evaluationContext, String::class.java)!!
                    }

                    if (StringUtils.isEmpty(field.function)
                        || StringUtils.containsNone(field.function, "as")
                    ) {
                        selectClause = "($selectClause) as `${field.name}`"
                    }

                    return@map selectClause
                }
                .toArray { size -> Array(size) { "" } }

            dataset.selectExpr(*selects)
        }

        return if (source.watermark == null)
            interimDataset
        else withWatermark(
            interimDataset,
            source.watermark as Source.Watermark
        )
    }

    private fun withWatermark(dataset: Dataset<Row>, waterMark: Source.Watermark) =
        dataset.withWatermark(waterMark.eventTimeColumn, waterMark.delayThreshold)
}