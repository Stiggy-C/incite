package io.openenterprise.incite.service

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.ImmutableMap
import io.openenterprise.incite.TestUtils
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.spark.sql.streaming.DataStreamWriterHolder
import io.openenterprise.testcontainers.containers.KafkaContainer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteJdbcThinDriver
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.UUIDSerializer
import org.assertj.core.util.Lists
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.FixMethodOrder
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.MethodSorters
import org.mockito.internal.util.collections.Sets
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.testcontainers.containers.PostgreSQLContainer
import java.util.*
import javax.inject.Inject

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore
@RunWith(SpringJUnit4ClassRunner::class)
class PipelineServiceImplTest {

    @Inject
    private lateinit var coroutineScope: CoroutineScope

    @Inject
    private lateinit var ignite: Ignite

    @Inject
    private lateinit var jdbcTemplate: JdbcTemplate

    @Inject
    private lateinit var kafkaContainer: KafkaContainer

    @Inject
    private lateinit var kafkaTemplate: KafkaTemplate<UUID, AwsDmsMessage>

    @Inject
    private lateinit var pipelineService: PipelineService

    @Inject
    private lateinit var postgreSQLContainer: PostgreSQLContainer<*>

    @Before
    fun before() {
        jdbcTemplate.update(
            "create table if not exists guest (id bigint primary key, membership_number varchar," +
                    " created_date_time timestamp with time zone, last_login_date_time timestamp with time zone)"
        )

        jdbcTemplate.update("insert into guest values (1, '2020324690', now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (2, '2021135985', now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (3, '2022031234', now(), now()) on conflict do nothing")
    }

    @Test
    fun aggregateMultipleTimesAndExceptionOccurred() {
        val aggregateId = UUID.randomUUID().toString()
        val embeddedIgniteSinkId = UUID.randomUUID()
        val kafkaTopic = "transactions_0"
        val igniteTable = "guest_transactions_0"

        coroutineScope.launch {
            runPipeline(aggregateId, embeddedIgniteSinkId, igniteTable, kafkaTopic)
        }

        Thread.sleep(2500)

        var exception: Exception? = null

        try {
            runPipeline(aggregateId, embeddedIgniteSinkId, igniteTable, kafkaTopic)
        } catch (e: Exception) {
            exception = e
        }

        assertNotNull(exception)
    }

    @Test
    fun aggregateNonStreamingSourceWithStreamingSource() {
        val aggregateId = UUID.randomUUID().toString()
        val embeddedIgniteSinkId = UUID.randomUUID()
        val kafkaTopic = "transactions_1"
        val igniteTable = "guest_transactions_1"

        val aggregate = runPipeline(aggregateId, embeddedIgniteSinkId, igniteTable, kafkaTopic)

        assertNotNull(aggregate.lastRunDateTime)

        val awsDmsMessage0 = AwsDmsMessage()
        awsDmsMessage0.data = ImmutableMap.of(
            "id",
            UUID.randomUUID().toString(),
            "membership_number",
            "2021135985",
            "sku",
            UUID.randomUUID().toString(),
            "price",
            "423.0",
            "created_date_time",
            "2022-01-01 02:34:00.000"
        )

        kafkaTemplate.send(kafkaTopic, UUID.randomUUID(), awsDmsMessage0)

        Thread.sleep(20000)

        val pipelineContext = pipelineService.getContext(aggregate.id!!)

        assertNotNull(pipelineContext)
        assertTrue(pipelineContext!!.writerHolders!!.stream().allMatch { it is DataStreamWriterHolder })
        assertTrue(pipelineContext.writerHolders!!.stream()
            .allMatch { (it as DataStreamWriterHolder).streamingQuery.isActive })

        val igniteCacheName = "SQL_PUBLIC_${igniteTable.uppercase()}"

        assertTrue(ignite.cache<Any, Any>(igniteCacheName).size() > 0)
    }

    @Test
    fun aggregateWithoutOverwritingAndCanResume() {
        val aggregateId = UUID.randomUUID().toString()
        val kafkaTopic = "transactions_2"
        val embeddedIgniteSinkId = UUID.randomUUID()
        val igniteTable = "guest_transactions_2"

        var aggregate = runPipeline(aggregateId, embeddedIgniteSinkId, igniteTable, kafkaTopic)

        assertNotNull(aggregate.lastRunDateTime)

        val awsDmsMessage0 = AwsDmsMessage()
        awsDmsMessage0.data = ImmutableMap.of(
            "id",
            UUID.randomUUID().toString(),
            "membership_number",
            "2021135985",
            "sku",
            UUID.randomUUID().toString(),
            "price",
            "423.0",
            "created_date_time",
            "2022-01-01 02:34:00.000"
        )

        kafkaTemplate.send(kafkaTopic, UUID.randomUUID(), awsDmsMessage0)

        Thread.sleep(5000)

        var pipelineContext = pipelineService.getContext(aggregate.id!!)

        pipelineContext!!.writerHolders!!.stream().filter { it is DataStreamWriterHolder }
            .map { it as DataStreamWriterHolder }.map { it.streamingQuery }.peek { it.processAllAvailable() }
            .forEach { it.stop() }

        aggregate = runPipeline(aggregateId, embeddedIgniteSinkId, igniteTable, kafkaTopic)

        val awsDmsMessage1 = AwsDmsMessage()
        awsDmsMessage1.data = ImmutableMap.of(
            "id",
            UUID.randomUUID().toString(),
            "membership_number",
            "2022031234",
            "sku",
            UUID.randomUUID().toString(),
            "price",
            "423.0",
            "created_date_time",
            "2022-03-03 10:00:00.000"
        )

        kafkaTemplate.send(kafkaTopic, UUID.randomUUID(), awsDmsMessage1)

        Thread.sleep(5000)

        pipelineContext = pipelineService.getContext(aggregate.id!!)

        assertNotNull(pipelineContext)
        assertTrue(pipelineContext!!.writerHolders!!.stream().allMatch { it is DataStreamWriterHolder })
        assertTrue(pipelineContext.writerHolders!!.stream()
            .allMatch { (it as DataStreamWriterHolder).streamingQuery.isActive })

        val igniteCacheName = "SQL_PUBLIC_${igniteTable.uppercase()}"

        Thread.sleep(20000)

        assertTrue(ignite.cache<Any, Any>(igniteCacheName).size() > 1)
    }

    private fun runPipeline(
        pipelineId: String, embeddedIgniteSinkId: UUID, igniteTable: String, kafkaTopic: String
    ): Pipeline {
        val rdbmsDatabase0 = RdbmsDatabase()
        rdbmsDatabase0.url = "jdbc:postgresql://host.testcontainers.internal:5432/${postgreSQLContainer.databaseName}"
        rdbmsDatabase0.driverClass = "org.postgresql.Driver"
        rdbmsDatabase0.password = postgreSQLContainer.password
        rdbmsDatabase0.username = postgreSQLContainer.username

        val jdbcSource = JdbcSource()
        jdbcSource.rdbmsDatabase = rdbmsDatabase0
        jdbcSource.query =
            "select g.id as guest_id, g.membership_number, g.created_date_time, g.last_login_date_time from guest g order by last_login_date_time desc"

        val kafkaCluster = KafkaCluster()
        kafkaCluster.servers = TestUtils.manipulateKafkaBootstrapServers(kafkaContainer)

        val kafkaSource = KafkaSource()
        kafkaSource.fields = Sets.newSet(
            Field("data.id", "#field as transaction_id"),
            Field("data.membership_number", "#field as membership_number"),
            Field("data.sku", "#field as sku"),
            Field("data.price", "#field as price"),
            Field(
                "data.created_date_time", "to_timestamp(#field, 'yyyy-MM-dd HH:mm:ss.SSS') as purchase_date_time"
            )
        )
        kafkaSource.kafkaCluster = kafkaCluster
        kafkaSource.startingOffset = KafkaSource.Offset.Earliest
        kafkaSource.topic = kafkaTopic
        kafkaSource.watermark = Source.Watermark("purchase_date_time", "5 minutes")

        val rdbmsDatabase1 = RdbmsDatabase()
        rdbmsDatabase1.driverClass = IgniteJdbcThinDriver::class.java.name
        rdbmsDatabase1.url = "jdbc:ignite:thin://host.testcontainers.internal:10800?lazy=true&queryEngine=h2"
        rdbmsDatabase1.username = "ignite"
        rdbmsDatabase1.password = "ignite"

        val igniteSink = IgniteSink()
        igniteSink.id = embeddedIgniteSinkId.toString()
        igniteSink.primaryKeyColumns = "transaction_id"
        igniteSink.rdbmsDatabase = rdbmsDatabase1
        igniteSink.table = igniteTable

        val embeddedIgniteSinkStreamingWrapper = StreamingWrapper(igniteSink)
        embeddedIgniteSinkStreamingWrapper.triggerInterval = 500L

        val join = Join()
        join.leftColumn = "membership_number"
        join.rightColumn = "membership_number"
        join.rightIndex = 1
        join.type = Join.Type.INNER

        val pipeline = Pipeline()
        pipeline.id = pipelineId
        pipeline.description = "Unit test"
        pipeline.joins = Lists.list(join)
        pipeline.sources = Lists.list(kafkaSource, jdbcSource)
        pipeline.sinks = Lists.list(embeddedIgniteSinkStreamingWrapper)

        return pipelineService.start(pipeline)
    }

    data class AwsDmsMessage(
        var data: MutableMap<String, Any> = HashMap(), var metadata: MutableMap<String, Any> = HashMap()
    )

    @TestConfiguration
    @Import(io.openenterprise.incite.TestConfiguration::class)
    class Configuration {

        @Bean
        protected fun kafkaTemplate(producerFactory: ProducerFactory<UUID, PipelineServiceImplTest.AwsDmsMessage>): KafkaTemplate<UUID, PipelineServiceImplTest.AwsDmsMessage> =
            KafkaTemplate(producerFactory)

        @Bean
        protected fun producerFactory(
            kafkaContainer: KafkaContainer, objectMapper: ObjectMapper
        ): ProducerFactory<UUID, AwsDmsMessage> {
            val bootstrapServers = TestUtils.manipulateKafkaBootstrapServers(kafkaContainer)

            val configurations = ImmutableMap.builder<String, Any>()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers).build()

            return DefaultKafkaProducerFactory(
                configurations,
                UUIDSerializer(),
                JsonSerializer(object : TypeReference<AwsDmsMessage>() {}, objectMapper)
            )
        }

    }
}