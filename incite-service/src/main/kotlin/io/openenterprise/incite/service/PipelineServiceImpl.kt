package io.openenterprise.incite.service

import com.google.common.collect.ImmutableMap
import io.openenterprise.incite.PipelineContext
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.incite.spark.sql.streaming.DatasetStreamingWriter
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.commons.lang3.BooleanUtils.isFalse
import org.apache.commons.lang3.ObjectUtils.isNotEmpty
import org.apache.commons.lang3.StringUtils
import org.apache.ignite.Ignite
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import scala.collection.JavaConversions
import scala.collection.Seq
// import scala.collection.JavaConversions
import java.time.Duration
import java.time.OffsetDateTime
import java.util.Objects.nonNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Lock
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.EntityNotFoundException

@Named
open class PipelineServiceImpl(
    @Inject private val datasetService: DatasetService,
    @Inject private val ignite: Ignite
) : PipelineService,
    AbstractAbstractMutableEntityServiceImpl<Pipeline, String>() {

    private val pipelineContexts: MutableMap<String, PipelineContext> = ConcurrentHashMap()

    private val pipelineLocks: MutableMap<String, Lock> = ConcurrentHashMap()

    override fun getContext(id: String): PipelineContext? =
        if (pipelineContexts.containsKey(id)) {
            val pipeline = pipelineContexts[id]

            pipeline!!.dataset ?: throw IllegalStateException()
            pipeline.datasetWriters ?: throw IllegalStateException()

            val datasetWriters = pipeline.datasetWriters!!
            val isStreaming = datasetWriters.stream().anyMatch { it is DatasetStreamingWriter }

            if (isStreaming) {
                val isStreamingQueryActive = datasetWriters.stream()
                    .filter { it is DatasetStreamingWriter }
                    .anyMatch { (it as DatasetStreamingWriter).streamingQuery.isActive }

                pipeline.status = if (isStreamingQueryActive) {
                    PipelineContext.Status.PROCESSING
                } else {
                    PipelineContext.Status.STOPPED
                }

                pipelineContexts[id] = pipeline
            }

            pipeline
        } else {
            // TODO Check other nodes for status of remote PipelineContext

            null
        }


    override fun start(pipeline: Pipeline): Pipeline {
        if (pipeline.id == null) {
            throw IllegalArgumentException("Aggregate.id can not be null")
        }

        // Step 0: Prep works
        assert(pipeline.sources.size > 0)
        assert(pipeline.joins.size > 0)

        val lock = ignite.reentrantLock(getLockKey(pipeline), true, true, true)
        val locked = lock.tryLock()

        if (isFalse(locked)) {
            throw IllegalStateException("Unable to acquire lock to aggregate")
        }

        pipelineLocks[pipeline.id!!] = lock

        val aggregateStartDateTime = OffsetDateTime.now()
        val isStreaming = pipeline.sources.stream().anyMatch { it is StreamingSource && it.streamingRead }

        if (nonNull(pipeline.lastRunDateTime)) {
            val duration = Duration.between(pipeline.lastRunDateTime, aggregateStartDateTime)
            val durationInMillis = duration.toMillis()

            if (durationInMillis < pipeline.fixedDelay) {
                throw IllegalStateException("Aggregate can not be re-run for another $durationInMillis milliseconds")
            }
        }

        val pipelineContext = PipelineContext(PipelineContext.Status.PROCESSING)
        pipelineContexts[pipeline.id!!] = pipelineContext

        // Step 1: Load all the org.apache.spark.sql.Dataset's from sources
        val variables: Map<String, Any> =
            if (pipeline.lastRunDateTime == null) {
                ImmutableMap.of()
            } else {
                ImmutableMap.of("lastRunDateTime", pipeline.lastRunDateTime as Any)
            }

        val datasets = datasetService.load(pipeline.sources, variables)

        // Step 2: Join others in datasets to datasets[0]
        val result = joinSources(datasets, pipeline.joins)

        pipelineContext.dataset = result

        // Step 3: Write joint dataset to sinks
        var exceptionOccurred: Exception? = null

        val writers = try {
            datasetService.write(result, pipeline.sinks, isStreaming)
        } catch (e: Exception) {
            exceptionOccurred = e

            throw e
        } finally {
            if (isFalse(isStreaming) || isNotEmpty(exceptionOccurred)) {
                pipelineContext.status = PipelineContext.Status.STOPPED

                pipelineLocks.remove(getLockKey(pipeline))
                lock.unlock()
            }
        }

        pipeline.lastRunDateTime = aggregateStartDateTime
        pipelineContext.datasetWriters = writers

        return pipeline
    }

    override fun stopStreaming(id: String): Boolean {
        val pipelineContext = getContext(id) ?: throw IllegalArgumentException()
        val datasetWriter = pipelineContext.datasetWriters ?: throw IllegalStateException()
        val isStreamingAggregate = datasetWriter.stream().anyMatch { it is DatasetStreamingWriter }

        if (isFalse(isStreamingAggregate)) {
            throw UnsupportedOperationException()
        }

        val aggregate = retrieve(id) ?: throw EntityNotFoundException()
        val lockKey = getLockKey(aggregate)
        val lock = pipelineLocks[lockKey] ?: throw IllegalStateException()

        val result = datasetWriter.stream()
            .filter {
                it is DatasetStreamingWriter
            }.peek {
                (it as DatasetStreamingWriter).streamingQuery.stop()
            }.map {
                (it as DatasetStreamingWriter).streamingQuery.isActive
            }.reduce(true) { t, u -> t && u }

        lock.unlock()

        pipelineLocks.remove(lockKey)

        return result
    }

    internal fun isStreaming(sources: List<Source>): Boolean {
        return sources.stream().anyMatch { it is StreamingSource && it.streamingRead }
    }

    internal fun joinSources(datasets: List<Dataset<Row>>, joins: List<Join>): Dataset<Row> {
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

    private fun getLockKey(pipeline: Pipeline): String {
        return "${Pipeline::class.java.name}#${pipeline.id}"
    }
}