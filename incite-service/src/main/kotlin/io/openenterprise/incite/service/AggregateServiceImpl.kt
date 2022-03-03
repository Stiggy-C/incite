package io.openenterprise.incite.service

import com.google.common.collect.ImmutableMap
import io.openenterprise.incite.AggregateContext
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.spark.sql.DatasetWriter
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.incite.spark.sql.streaming.DatasetStreamingWriter
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.commons.lang3.BooleanUtils.isFalse
import org.apache.commons.lang3.StringUtils
import org.apache.ignite.Ignite
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.expression.spel.support.StandardEvaluationContext
import scala.collection.JavaConversions
import java.time.Duration
import java.time.OffsetDateTime
import java.util.*
import java.util.Objects.isNull
import java.util.Objects.nonNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Lock
import java.util.stream.Collectors
import javax.inject.Inject
import javax.inject.Named

@Named
class AggregateServiceImpl(
    @Inject private val datasetService: DatasetService,
    @Inject private val ignite: Ignite,
    @Inject private val spelExpressionParser: SpelExpressionParser,
) : AggregateService,
    AbstractAbstractMutableEntityServiceImpl<Aggregate, String>() {

    companion object {

        private val LOG = LoggerFactory.getLogger(AggregateServiceImpl::class.java)
    }

    private val aggregateContexts: MutableMap<String, AggregateContext> = ConcurrentHashMap()

    private val aggregateLocks: MutableMap<String, Lock> = ConcurrentHashMap()

    override fun aggregate(aggregate: Aggregate): Aggregate {
        if (aggregate.id == null) {
            throw IllegalArgumentException("Aggregate.id can not be null")
        }

        // Step 0: Prep works
        assert(aggregate.sources.size > 0)
        assert(aggregate.joins.size > 0)

        val lock = ignite.reentrantLock(getLockName(aggregate), true, true, true)
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

        // Step 1: Load all the org.apache.spark.sql.Dataset's from sources
        val variables: Map<String, Any> =
            if (aggregate.lastRunDateTime == null) {
                ImmutableMap.of()
            } else {
                ImmutableMap.of("lastRunDateTime", aggregate.lastRunDateTime as Any)
            }

        val datasets = loadSources(aggregate.sources, variables)

        // Step 2: Join others in datasets to datasets[0]
        val result = joinSources(datasets, aggregate.joins)

        // Step 3: Write joint dataset to sinks
        val writers = writeSinks(result, aggregate.sinks, isStreaming)

        val aggregateContext = AggregateContext(result, writers)
        aggregateContexts[aggregate.id!!] = aggregateContext

        aggregate.lastRunDateTime = aggregateStartDateTime

        if (isFalse(isStreaming)) {
            lock.unlock()

            aggregateLocks.remove(getLockName(aggregate))
        }

        return aggregate
    }

    override fun getAggregateContext(id: String): AggregateContext? {
        return if (aggregateContexts.containsKey(id)) aggregateContexts[id] else null
    }

    override fun stopStreamingAggregate(id: String): Boolean {
        if (isNull(getAggregateContext(id))) {
            throw IllegalStateException()
        }

        val aggregateContext = getAggregateContext(id)!!

        val isStreamingAggregate = aggregateContext.datasetWriters.stream().anyMatch { it is DatasetStreamingWriter }

        if (isFalse(isStreamingAggregate)) {
            throw UnsupportedOperationException()
        }

        val result = aggregateContext.datasetWriters.stream()
            .filter {
                it is DatasetStreamingWriter
            }.peek {
                (it as DatasetStreamingWriter).streamingQuery.stop()
            }.map {
                (it as DatasetStreamingWriter).streamingQuery.isActive
            }.reduce(true) { t, u -> t && u }

        return result
    }

    internal fun joinSources(datasets: List<Dataset<Row>>, joins: List<Join>): Dataset<Row> {
        var result = datasets[0]

        joins.stream()
            .forEach {
                val leftDataset = result.alias("left")
                val rightDataset = datasets[it.rightIndex].alias("right")
                val leftColumn = it.leftColumn
                val rightColumn = it.rightColumn

                val columns = if (StringUtils.equalsIgnoreCase(leftColumn, rightColumn)) {
                    JavaConversions.asScalaBuffer(listOf(it.leftColumn)).seq()
                } else {
                    JavaConversions.asScalaBuffer(listOf("left.${it.leftColumn}", "right.${it.rightColumn}")).seq()
                }

                result = leftDataset.join(rightDataset, columns, it.type.name)
            }

        return result
    }

    internal fun loadSources(sources: List<Source>, variables: Map<String, *>): List<Dataset<Row>> {
        return sources.stream()
            .map {
                when (it) {
                    is JdbcSource -> {
                        manipulateSqlQuery(it, variables)
                    }
                    else -> it
                }
            }
            .map {
                datasetService.load(it)
            }
            .collect(
                Collectors.toList()
            )
    }

    internal fun writeSinks(result: Dataset<Row>, sinks: List<Sink>, streamingWrite: Boolean): Set<DatasetWriter<*>> {
        @Suppress("NAME_SHADOWING")
        val sinks = if (streamingWrite) {
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
                    is NonStreamingSink -> {
                        datasetService.write(result, it)
                    }
                    is StreamingSink -> {
                        datasetService.write(result, it)
                    }
                    else -> throw UnsupportedOperationException()
                }
            }
            .collect(Collectors.toSet())

        return writers
    }

    private fun getLockName(aggregate: Aggregate): String {
        return "${Aggregate::class.java.name}#${aggregate.id}"
    }

    private fun manipulateSqlQuery(jdbcSource: JdbcSource, variables: Map<String, *>): JdbcSource {
        val source = jdbcSource.clone() as JdbcSource

        val evaluationContext = StandardEvaluationContext()
        evaluationContext.setVariables(variables)

        var query: String = source.query
        val tokens = StringUtils.split(query, " ")
        val expressionTokens = Arrays.stream(tokens)
            .filter { token: String? ->
                StringUtils.startsWith(
                    token,
                    "#"
                )
            }
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
}