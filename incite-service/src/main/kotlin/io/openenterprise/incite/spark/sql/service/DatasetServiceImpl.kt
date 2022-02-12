package io.openenterprise.incite.spark.sql.service

import io.openenterprise.ignite.spark.IgniteDataFrameConstants
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.spark.sql.DatasetNonStreamingWriter
import io.openenterprise.incite.spark.sql.streaming.DatasetStreamingWriter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.spark.sql.*
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.expression.spel.support.StandardEvaluationContext
import java.util.stream.Collectors
import javax.inject.Inject
import javax.inject.Named

@Named
class DatasetServiceImpl(
    @Inject private val coroutineScope: CoroutineScope,
    @Value("\${io.openenterprise.incite.spark.checkpoint-location:./spark-checkpoints}") private val sparkCheckpointLocation: String,
    @Inject private val sparkSession: SparkSession,
    @Inject private val spelExpressionParser: SpelExpressionParser
) : DatasetService {

    companion object {

        private val LOG = LoggerFactory.getLogger(DatasetServiceImpl::class.java)
    }

    override fun load(source: Source): Dataset<Row> {
        return when (source) {
            is KafkaSource -> load(source)
            is JdbcSource -> load(source)
            else -> throw UnsupportedOperationException()
        }
    }

    override fun write(
        dataset: Dataset<Row>,
        sink: StreamingSink
    ): DatasetStreamingWriter {
        val trigger = when (sink.triggerType) {
            StreamingSink.TriggerType.CONTINUOUS -> {
                Trigger.Continuous(sink.triggerInterval)
            }
            StreamingSink.TriggerType.ONCE -> {
                Trigger.Once()
            }
            StreamingSink.TriggerType.PROCESSING_TIME -> {
                Trigger.ProcessingTime(sink.triggerInterval)
            }
            else -> {
                throw UnsupportedOperationException()
            }
        }

        var dataStreamWriter: DataStreamWriter<Row> = when (sink) {
            is KafkaSink -> TODO("Not yet implemented")
            is StreamingWrapper -> {
                dataset.writeStream()
                    .trigger(trigger)
                    .foreachBatch { batch, batchId ->
                        LOG.info("About to write dataset of batch {} to non-streaming sink, {}", batchId, sink)

                        write(batch, sink.nonStreamingSink)
                    }
            }
            else -> throw UnsupportedOperationException()
        }

        dataStreamWriter = dataStreamWriter
            .option("checkpointLocation", "$sparkCheckpointLocation/${sink.id}")
            .outputMode(sink.outputMode.name.lowercase())

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
            is EmbeddedIgniteSink -> {
                dataset.write()
                    .format(IgniteDataFrameConstants.FORMAT)
                    .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), sink.primaryKeyColumns)
                    .option(IgniteDataFrameSettings.OPTION_TABLE(), sink.table)
            }
            is JdbcSink -> {
                dataset.write()
                    .format("jdbc")
                    .option("dbtable", sink.table)
                    .option("url", sink.rdbmsDatabase.url)
                    .option("user", sink.rdbmsDatabase.username)
                    .option("password", sink.rdbmsDatabase.password)
            }
            else -> {
                throw UnsupportedOperationException()
            }
        }

        dataFrameWriter = dataFrameWriter.mode(sink.saveMode)

        dataFrameWriter.save()

        return DatasetNonStreamingWriter(dataFrameWriter)
    }

    private fun load(kafkaSource: KafkaSource): Dataset<Row> {
        var dataset: Dataset<Row> = if (kafkaSource.streamingRead) sparkSession.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaSource.kafkaCluster.servers)
            .option("startingOffsets", kafkaSource.startingOffset)
            .option("subscribe", kafkaSource.topic)
            .load() else sparkSession.read().format("kafka")
            .option("kafka.bootstrap.servers", kafkaSource.kafkaCluster.servers)
            .option("startingOffsets", "earliest")
            .option("subscribe", kafkaSource.topic)
            .load()

        dataset = dataset.selectExpr("cast(value as STRING)")

        // As of Dec 11, 2021, only support JSON message for now
        if (CollectionUtils.isNotEmpty(kafkaSource.fields)) {
            val selects: Array<String> = kafkaSource.fields!!.stream()
                .map { field ->
                    var thisSelectClause = "get_json_object(value, '$." + field.name + "')"

                    if (StringUtils.isNotEmpty(field.function)) {
                        val evaluationContext = StandardEvaluationContext()
                        evaluationContext.setVariable("field", thisSelectClause)
                        val functionTokens = field.function!!.split("#field").stream()
                            .map { token -> "\"" + token + "\"" }
                            .collect(Collectors.toList())
                        for (i in 0 until functionTokens.size) {
                            var token = functionTokens[i]
                            token = when (i) {
                                0 -> {
                                    "$token + "
                                }
                                functionTokens.size - 1 -> {
                                    " + $token"
                                }
                                else -> {
                                    " + $token + "
                                }
                            }
                            functionTokens[i] = token
                        }

                        thisSelectClause = spelExpressionParser.parseExpression(
                            java.lang.String.join("#field", functionTokens)
                        ).getValue(evaluationContext, String::class.java)!!
                    }

                    if (StringUtils.isEmpty(field.function)
                        || StringUtils.containsNone(field.function, "as")
                    ) {
                        thisSelectClause = "($thisSelectClause) as `${field.name}`"
                    }

                    return@map thisSelectClause
                }
                .toArray { size -> Array(size) { "" } }

            dataset = dataset.selectExpr(*selects)
        }

        return if (kafkaSource.watermark == null) dataset else withWatermark(
            dataset,
            kafkaSource.watermark as Source.Watermark
        )
    }

    private fun load(jdbcSource: JdbcSource): Dataset<Row> {
        val dataset: Dataset<Row> = sparkSession.read()
            .format("jdbc")
            .option("query", jdbcSource.query)
            .option("driver", jdbcSource.rdbmsDatabase.driverClass)
            .option("url", jdbcSource.rdbmsDatabase.url)
            .option("user", jdbcSource.rdbmsDatabase.username)
            .option("password", jdbcSource.rdbmsDatabase.password)
            .load()

        return if (jdbcSource.watermark == null) dataset else withWatermark(
            dataset,
            jdbcSource.watermark as Source.Watermark
        )
    }

    private fun withWatermark(dataset: Dataset<Row>, waterMark: Source.Watermark) =
        dataset.withWatermark(waterMark.eventTimeColumn, waterMark.delayThreshold)

}