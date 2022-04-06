package io.openenterprise.incite.service

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.google.common.collect.ImmutableMap
import io.openenterprise.ignite.spark.IgniteContext
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.data.repository.AggregateRepository
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.incite.spark.service.DatasetServiceImplTest
import io.openenterprise.incite.spark.sql.streaming.DatasetStreamingWriter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import org.apache.ignite.Ignition
import org.apache.ignite.cluster.ClusterState
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.UUIDSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.assertj.core.util.Lists
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.mockito.internal.util.collections.Sets
import org.postgresql.ds.PGSimpleDataSource
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.DependsOn
import org.springframework.context.annotation.Primary
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.util.*
import javax.inject.Inject
import javax.sql.DataSource
import kotlin.collections.HashMap

@RunWith(SpringJUnit4ClassRunner::class)
class AggregateServiceImplTest {

    @Inject
    private lateinit var aggregateService: AggregateService

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
            runAggregate(aggregateId, embeddedIgniteSinkId, igniteTable, kafkaTopic)
        }

        Thread.sleep(2500)

        var exception: Exception? = null

        try {
            runAggregate(aggregateId, embeddedIgniteSinkId, igniteTable, kafkaTopic)
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

        val aggregate = runAggregate(aggregateId, embeddedIgniteSinkId, igniteTable, kafkaTopic)

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

        val aggregateContext = aggregateService.getContext(aggregate.id!!)

        assertNotNull(aggregateContext)
        assertTrue(aggregateContext!!.datasetWriters.stream().allMatch { it is DatasetStreamingWriter })
        assertTrue(
            aggregateContext.datasetWriters.stream()
                .allMatch { (it as DatasetStreamingWriter).streamingQuery.isActive })

        val igniteCacheName = "SQL_PUBLIC_${igniteTable.uppercase()}"

        assertTrue(ignite.cache<Any, Any>(igniteCacheName).size() > 0)
    }

    @Test
    fun aggregateWithoutOverwritingAndCanResume() {
        val aggregateId = UUID.randomUUID().toString()
        val kafkaTopic = "transactions_2"
        val embeddedIgniteSinkId = UUID.randomUUID()
        val igniteTable = "guest_transactions_2"

        var aggregate = runAggregate(aggregateId, embeddedIgniteSinkId, igniteTable, kafkaTopic)

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

        var aggregateContext = aggregateService.getContext(aggregate.id!!)

        aggregateContext!!.datasetWriters.stream()
            .filter { it is DatasetStreamingWriter }
            .map { it as DatasetStreamingWriter }
            .map { it.streamingQuery }
            .peek { it.processAllAvailable() }
            .forEach { it.stop() }

        aggregate = runAggregate(aggregateId, embeddedIgniteSinkId, igniteTable, kafkaTopic)

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

        aggregateContext = aggregateService.getContext(aggregate.id!!)

        assertNotNull(aggregateContext)
        assertTrue(aggregateContext!!.datasetWriters.stream().allMatch { it is DatasetStreamingWriter })
        assertTrue(
            aggregateContext.datasetWriters.stream()
                .allMatch { (it as DatasetStreamingWriter).streamingQuery.isActive })

        val igniteCacheName = "SQL_PUBLIC_${igniteTable.uppercase()}"

        Thread.sleep(20000)

        assertTrue(ignite.cache<Any, Any>(igniteCacheName).size() > 1)
    }

    private fun runAggregate(
        aggregateId: String,
        embeddedIgniteSinkId: UUID,
        igniteTable: String,
        kafkaTopic: String
    ): Aggregate {
        val rdbmsDatabase = RdbmsDatabase()
        rdbmsDatabase.url = postgreSQLContainer.jdbcUrl
        rdbmsDatabase.driverClass = "org.postgresql.Driver"
        rdbmsDatabase.password = postgreSQLContainer.password
        rdbmsDatabase.username = postgreSQLContainer.username

        val jdbcSource = JdbcSource()
        jdbcSource.rdbmsDatabase = rdbmsDatabase
        jdbcSource.query =
            "select g.id as guest_id, g.membership_number, g.created_date_time, g.last_login_date_time from guest g order by last_login_date_time desc"

        val kafkaCluster = KafkaCluster()
        kafkaCluster.servers = kafkaContainer.bootstrapServers

        val kafkaSource = KafkaSource()
        kafkaSource.fields = Sets.newSet(
            Field("data.id", "#field as transaction_id"),
            Field("data.membership_number", "#field as membership_number"),
            Field("data.sku", "#field as sku"),
            Field("data.price", "#field as price"),
            Field(
                "data.created_date_time",
                "to_timestamp(#field, 'yyyy-MM-dd HH:mm:ss.SSS') as purchase_date_time"
            )
        )
        kafkaSource.kafkaCluster = kafkaCluster
        kafkaSource.startingOffset = "earliest"
        kafkaSource.topic = kafkaTopic
        kafkaSource.watermark = Source.Watermark("purchase_date_time", "5 minutes")

        val embeddedIgniteSink = EmbeddedIgniteSink()
        embeddedIgniteSink.id = embeddedIgniteSinkId
        embeddedIgniteSink.primaryKeyColumns = "transaction_id"
        embeddedIgniteSink.table = igniteTable

        val embeddedIgniteSinkStreamingWrapper = StreamingWrapper(embeddedIgniteSink)
        embeddedIgniteSinkStreamingWrapper.triggerInterval = 500L

        val join = Join()
        join.leftColumn = "membership_number"
        join.rightColumn = "membership_number"
        join.rightIndex = 1
        join.type = Join.Type.INNER

        val aggregate = Aggregate()
        aggregate.id = aggregateId
        aggregate.description = "Unit test"
        aggregate.joins = Lists.list(join)
        aggregate.sources = Lists.list(kafkaSource, jdbcSource)
        aggregate.sinks = Lists.list(embeddedIgniteSinkStreamingWrapper)

        return aggregateService.aggregate(aggregate)
    }

    data class AwsDmsMessage(
        var data: MutableMap<String, Any> = HashMap(),
        var metadata: MutableMap<String, Any> = HashMap()
    )

    @TestConfiguration
    @ComponentScan(
        value = [
            "io.openenterprise.incite.spark.sql.service", "io.openenterprise.springframework.context"
        ]
    )
    class Configuration {

        @Bean
        protected fun aggregateRepository(): AggregateRepository = Mockito.mock(AggregateRepository::class.java)


        @Bean
        protected fun aggregateService(
            datasetService: DatasetService, ignite: Ignite, spelExpressionParser: SpelExpressionParser
        ): AggregateService = AggregateServiceImpl(datasetService, ignite, spelExpressionParser)


        @Bean
        protected fun coroutineScope(): CoroutineScope = CoroutineScope(Dispatchers.Default)


        @Bean
        protected fun dataSource(postgreSQLContainer: PostgreSQLContainer<*>): DataSource {
            val datasource = PGSimpleDataSource()
            datasource.setUrl(postgreSQLContainer.jdbcUrl)

            datasource.user = postgreSQLContainer.username
            datasource.password = postgreSQLContainer.password

            return datasource
        }

        @Bean
        protected fun ignite(applicationContext: ApplicationContext): Ignite {
            val igniteConfiguration = IgniteConfiguration()
            igniteConfiguration.igniteInstanceName = DatasetServiceImplTest::class.java.simpleName

            return Ignition.getOrStart(igniteConfiguration)
        }

        @Bean
        protected fun igniteCluster(ignite: Ignite): IgniteCluster {
            val igniteCluster = ignite.cluster()

            try {
                return igniteCluster
            } finally {
                igniteCluster.state(ClusterState.ACTIVE)
            }
        }

        @Bean
        @ConditionalOnBean(Ignite::class)
        @DependsOn("applicationContextUtils", "sparkSession")
        fun igniteContext(applicationContext: ApplicationContext): IgniteContext {
            val sparkSession = applicationContext.getBean("sparkSession", SparkSession::class.java)

            return IgniteContext(sparkSession.sparkContext())
        }

        @Bean
        @Primary
        @Qualifier("igniteSparkSession")
        protected fun igniteSparkSession(igniteContext: IgniteContext): SparkSession =
            IgniteSparkSession(igniteContext, igniteContext.sqlContext().sparkSession())


        @Bean
        protected fun jdbcTemplate(datasource: DataSource): JdbcTemplate = JdbcTemplate(datasource)


        @Bean
        protected fun kafkaContainer(): KafkaContainer {
            val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
            kafkaContainer.start()

            return kafkaContainer
        }

        @Bean
        protected fun kafkaTemplate(producerFactory: ProducerFactory<UUID, AwsDmsMessage>): KafkaTemplate<UUID, AwsDmsMessage> =
            KafkaTemplate(producerFactory)

        @Bean
        fun objectMapper(): ObjectMapper = ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .findAndRegisterModules()
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)

        @Bean
        protected fun postgreSQLContainer(): PostgreSQLContainer<*> {
            val postgreSQLContainer: PostgreSQLContainer<*> =
                PostgreSQLContainer<PostgreSQLContainer<*>>("postgres:latest")
            postgreSQLContainer.withPassword("test_password")
            postgreSQLContainer.withUsername("test_user")

            postgreSQLContainer.start()

            return postgreSQLContainer
        }

        @Bean
        protected fun producerFactory(
            kafkaContainer: KafkaContainer,
            objectMapper: ObjectMapper
        ): ProducerFactory<UUID, AwsDmsMessage> {
            val configurations = ImmutableMap.builder<String, Any>()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.bootstrapServers)
                .build()

            return DefaultKafkaProducerFactory(
                configurations,
                UUIDSerializer(),
                JsonSerializer(object : TypeReference<AwsDmsMessage>() {}, objectMapper)
            )
        }

        @Bean
        @Order(Ordered.HIGHEST_PRECEDENCE)
        protected fun sparkSession(): SparkSession {
            return SparkSession.builder()
                .appName(DatasetServiceImplTest::class.java.simpleName)
                .master("local[*]")
                .config("spark.sql.streaming.schemaInference", true)
                .orCreate
        }

        @Bean
        protected fun spelExpressionParser(): SpelExpressionParser = SpelExpressionParser()
    }
}