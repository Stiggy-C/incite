package io.openenterprise.incite.service

import com.google.common.collect.ImmutableMap
import io.openenterprise.incite.AggregateContext
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.spark.sql.DatasetWriter
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
import org.slf4j.LoggerFactory
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.expression.spel.support.StandardEvaluationContext
import scala.collection.JavaConversions
import scala.collection.Seq
// import scala.collection.JavaConversions
import java.time.Duration
import java.time.OffsetDateTime
import java.util.*
import java.util.Objects.nonNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Lock
import java.util.stream.Collectors
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.EntityNotFoundException

@Named
open class AggregateServiceImpl(
    @Inject private val datasetService: DatasetService,
    @Inject private val ignite: Ignite
) : AggregateService,
    AbstractAbstractMutableEntityServiceImpl<Aggregate, String>() {

    private val aggregateContexts: MutableMap<String, AggregateContext> = ConcurrentHashMap()

    private val aggregateLocks: MutableMap<String, Lock> = ConcurrentHashMap()

    override fun aggregate(aggregate: Aggregate): Aggregate {
        if (aggregate.id == null) {
            throw IllegalArgumentException("Aggregate.id can not be null")
        }

        // Step 0: Prep works
        assert(aggregate.sources.size > 0)
        assert(aggregate.joins.size > 0)

        val lock = ignite.reentrantLock(getLockKey(aggregate), true, true, true)
        val locked = lock.tryLock()

        if (isFalse(locked)) {
            throw IllegalStateException("Unable to acquire lock to aggregate")
        }

        aggregateLocks[aggregate.id!!] = lock

        val aggregateStartDateTime = OffsetDateTime.now()
        val isStreaming = aggregate.sources.stream().anyMatch { it is StreamingSource && it.streamingRead }

        if (nonNull(aggregate.lastRunDateTime)) {
            val duration = Duration.between(aggregate.lastRunDateTime, aggregateStartDateTime)
            val durationInMillis = duration.toMillis()

            if (durationInMillis < aggregate.fixedDelay) {
                throw IllegalStateException("Aggregate can not be re-run for another $durationInMillis milliseconds")
            }
        }

        val aggregateContext = AggregateContext(AggregateContext.Status.PROCESSING)
        aggregateContexts[aggregate.id!!] = aggregateContext

        // Step 1: Load all the org.apache.spark.sql.Dataset's from sources
        val variables: Map<String, Any> =
            if (aggregate.lastRunDateTime == null) {
                ImmutableMap.of()
            } else {
                ImmutableMap.of("lastRunDateTime", aggregate.lastRunDateTime as Any)
            }

        val datasets = datasetService.load(aggregate.sources, variables)

        // Step 2: Join others in datasets to datasets[0]
        val result = joinSources(datasets, aggregate.joins)

        aggregateContext.dataset = result

        // Step 3: Write joint dataset to sinks
        var exceptionOccurred: Exception? = null

        val writers = try {
            datasetService.write(result, aggregate.sinks, isStreaming)
        } catch (e: Exception) {
            exceptionOccurred = e

            throw e
        } finally {
            if (isFalse(isStreaming) || isNotEmpty(exceptionOccurred)) {
                aggregateContext.status = AggregateContext.Status.STOPPED

                aggregateLocks.remove(getLockKey(aggregate))
                lock.unlock()
            }
        }

        aggregate.lastRunDateTime = aggregateStartDateTime
        aggregateContext.datasetWriters = writers

        return aggregate
    }

    override fun getContext(id: String): AggregateContext? {
        return if (aggregateContexts.containsKey(id)) aggregateContexts[id] else null
    }

    override fun stopStreaming(id: String): Boolean {
        val aggregateContext = getContext(id) ?: throw IllegalArgumentException()
        val isStreamingAggregate = aggregateContext.datasetWriters.stream().anyMatch { it is DatasetStreamingWriter }

        if (isFalse(isStreamingAggregate)) {
            throw UnsupportedOperationException()
        }

        val aggregate = retrieve(id) ?: throw EntityNotFoundException()
        val lockKey = getLockKey(aggregate)
        val lock = aggregateLocks[lockKey] ?: throw IllegalStateException()

        val result = aggregateContext.datasetWriters.stream()
            .filter {
                it is DatasetStreamingWriter
            }.peek {
                (it as DatasetStreamingWriter).streamingQuery.stop()
            }.map {
                (it as DatasetStreamingWriter).streamingQuery.isActive
            }.reduce(true) { t, u -> t && u }

        lock.unlock()

        aggregateLocks.remove(lockKey)

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

    private fun getLockKey(aggregate: Aggregate): String {
        return "${Aggregate::class.java.name}#${aggregate.id}"
    }
}