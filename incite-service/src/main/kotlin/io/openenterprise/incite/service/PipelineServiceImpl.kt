package io.openenterprise.incite.service

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Maps
import io.openenterprise.incite.PipelineContext
import io.openenterprise.incite.PipelineContextUtils
import io.openenterprise.incite.PipelineContextUtils.Companion.getPipelineContexts
import io.openenterprise.incite.PipelineContextUtils.Companion.getPipelineContextsLookup
import io.openenterprise.incite.data.domain.Join
import io.openenterprise.incite.data.domain.Pipeline
import io.openenterprise.incite.data.domain.StreamingSource
import io.openenterprise.incite.spark.sql.WriterHolder
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.incite.spark.sql.streaming.DataStreamWriterHolder
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.commons.lang3.BooleanUtils.isFalse
import org.apache.commons.lang3.StringUtils
import org.apache.ignite.Ignite
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import scala.collection.JavaConversions
import scala.collection.Seq
import java.time.Duration
import java.time.OffsetDateTime
import java.util.*
import java.util.Objects.nonNull
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.EntityNotFoundException

@Named
open class PipelineServiceImpl(
    @Inject private val datasetService: DatasetService,
    @Inject private val ignite: Ignite
) : PipelineService,
    AbstractAbstractMutableEntityServiceImpl<Pipeline, String>() {

    override fun getContext(id: String): PipelineContext? =
        if (getPipelineContexts(ignite).containsKey(id)) {
            val pipeline = getPipelineContexts(ignite)[id]

            pipeline!!.dataset ?: throw IllegalStateException()
            pipeline.writerHolders ?: throw IllegalStateException()

            pipeline
        } else {
            null
        }

    override fun start(pipeline: Pipeline): Pipeline {
        if (pipeline.id == null) {
            throw IllegalArgumentException("pipeline.id can not be null")
        }

        assert(pipeline.sources.size > 0)

        val lock = ignite.reentrantLock(getLockKey(pipeline), true, true, true)
        val locked = lock.tryLock()

        if (isFalse(locked)) {
            throw IllegalStateException("Pipeline can not be started as unable to acquire lock")
        }

        val isStreaming = pipeline.sources.stream().anyMatch { it is StreamingSource && it.streamingRead }

        /*
         * Step 0: Prep
         * Step 1: Load all the org.apache.spark.sql.Dataset's defined in pipeline.sources
         * Step 2: Aggregate according to pipeline.joins
         * Step 3: Write result to sinks
         */
        if (isStreaming) {
            stream(pipeline)
        } else {
            run(pipeline)
        }

        lock.unlock()

        return pipeline
    }

    override fun stopStreaming(id: String): Boolean {
        val pipelineContext = getContext(id) ?: throw IllegalArgumentException()
        val datasetWriter = pipelineContext.writerHolders ?: throw IllegalStateException()
        val isStreamingAggregate = datasetWriter.stream().anyMatch { it is DataStreamWriterHolder }

        if (isFalse(isStreamingAggregate)) {
            throw UnsupportedOperationException()
        }

        val pipeline = retrieve(id) ?: throw EntityNotFoundException()
        val lockKey = getLockKey(pipeline)
        val lock = ignite.reentrantLock(lockKey, true, true, true)

        if (!lock.isLocked) {
            throw IllegalStateException("Pipeline lock was not locked")
        }

        val result = datasetWriter.stream()
            .filter {
                it is DataStreamWriterHolder
            }.peek {
                (it as DataStreamWriterHolder).streamingQuery.stop()
            }.map {
                (it as DataStreamWriterHolder).streamingQuery.isActive
            }.reduce(true) { t, u -> t && u }

        lock.unlock()

        return result
    }

    internal fun aggregate(datasets: List<Dataset<Row>>, joins: List<Join>): Dataset<Row> {
        var result = datasets[0]

        joins.stream()
            .forEach {
                val leftDataset = result.alias("left")
                val rightDataset = datasets[it.rightIndex].alias("right")
                val leftColumn = it.leftColumn
                val rightColumn = it.rightColumn

                result = if (StringUtils.equalsIgnoreCase(leftColumn, rightColumn)) {
                    val column: Seq<String> = JavaConversions.asScalaBuffer(listOf(it.leftColumn)).seq()

                    leftDataset.join(rightDataset, column, it.type.name)
                } else {
                    val columns = Column("left.`$leftColumn`").equalTo(Column("right.`$rightColumn`"))

                    leftDataset.join(rightDataset, columns, it.type.name)
                }
            }

        return result
    }

    private fun aggregate(pipeline: Pipeline, pipelineContext: PipelineContext): Dataset<Row> =
        aggregate(load(pipeline, pipelineContext), pipeline.joins)

    private fun getLockKey(pipeline: Pipeline): String = "${Pipeline::class.java.name}#${pipeline.id}"

    private fun load(pipeline: Pipeline, pipelineContext: PipelineContext): List<Dataset<Row>> =
        datasetService.load(pipeline.sources, pipelineContext.variables)

    private fun run(pipeline: Pipeline) {
        val pipelineContext = setup(pipeline)
        pipelineContext.dataset = aggregate(pipeline, pipelineContext)
        pipelineContext.writerHolders = write(pipeline, pipelineContext)

        tearDown(pipeline, pipelineContext)
    }

    private fun setup(pipeline: Pipeline, variables: MutableMap<String, Any> = HashMap()): PipelineContext {
        val startDateTime = OffsetDateTime.now()
        variables["startDateTime"] = startDateTime

        if (nonNull(pipeline.lastRunDateTime)) {
            val duration = Duration.between(pipeline.lastRunDateTime, startDateTime)
            val durationInMillis = duration.toMillis()

            if (durationInMillis < pipeline.fixedDelay) {
                throw IllegalStateException("Pipeline can not be started for another $durationInMillis milliseconds")
            }

            variables["lastRunDateTime"] = pipeline.lastRunDateTime!!
        }

        val pipelineContext = PipelineContext(
            startDateTime = startDateTime,
            variables = ImmutableMap.copyOf(variables)
        )

        getPipelineContexts(ignite)[pipeline.id!!] = pipelineContext

        return pipelineContext
    }

    private fun stream(pipeline: Pipeline) {
        val pipelineContext = setup(pipeline)
        pipelineContext.dataset = aggregate(pipeline, pipelineContext)
        pipelineContext.writerHolders = streamingWrite(pipeline, pipelineContext)

        getPipelineContexts(ignite)[pipeline.id as String] = pipelineContext

        pipelineContext.writerHolders?.stream()!!
            .filter { it is DataStreamWriterHolder }
            .forEach {
                getPipelineContextsLookup(ignite)[(it as DataStreamWriterHolder).streamingQuery.id()] = pipelineContext
            }

        tearDown(pipeline, pipelineContext)
    }

    private fun streamingWrite(pipeline: Pipeline, pipelineContext: PipelineContext): Set<WriterHolder<*>> =
        try {
            datasetService.write(pipelineContext.dataset!!, pipeline.sinks, true)
        } catch (e: Exception) {
            throw e
        }

    private fun tearDown(pipeline: Pipeline, pipelineContext: PipelineContext) {
        pipeline.lastRunDateTime = pipelineContext.startDateTime

        pipelineContext.writerHolders?.stream()!!
            .filter { it is DataStreamWriterHolder }
            .forEach {
                getPipelineContextsLookup(ignite)[(it as DataStreamWriterHolder).streamingQuery.id()] = pipelineContext
            }
    }

    private fun write(pipeline: Pipeline, pipelineContext: PipelineContext): Set<WriterHolder<*>> {
        pipelineContext.status = PipelineContext.Status.PROCESSING

        var exception: Exception? = null

        try {
            return datasetService.write(pipelineContext.dataset!!, pipeline.sinks, false)
        } catch (e: Exception) {
            exception = e

            throw e
        } finally {
            val status = if (exception == null) {
                PipelineContext.Status.STOPPED
            } else {
                PipelineContext.Status.FAILED
            }

            pipelineContext.status = status
        }
    }
}