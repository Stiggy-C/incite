package io.openenterprise.incite.ml.service

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.google.common.collect.ImmutableMap
import io.openenterprise.ignite.spark.IgniteContext
import io.openenterprise.incite.data.repository.AggregateRepository
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
import io.openenterprise.incite.service.AggregateServiceImplTest
import io.openenterprise.incite.spark.service.DatasetServiceImplTest
import io.openenterprise.incite.spark.sql.service.DatasetService
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CachingProvider
import org.apache.ignite.cluster.ClusterState
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.UUIDSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.mockito.Mockito
import org.postgresql.ds.PGSimpleDataSource
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
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
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.util.*
import javax.cache.Cache
import javax.cache.configuration.MutableConfiguration
import javax.sql.DataSource

abstract class AbstractMLServiceImplTest {

    @org.springframework.context.annotation.Configuration
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
        protected fun kafkaTemplate(producerFactory: ProducerFactory<UUID, Map<String, Any>>): KafkaTemplate<UUID, Map<String, Any>> =
            KafkaTemplate(producerFactory)

        @Bean("mlModelsCache")
        fun mlModelsCache(): Cache<UUID, File> {
            val mutableConfiguration: MutableConfiguration<UUID, File> =
                MutableConfiguration<UUID, File>()
            mutableConfiguration.setTypes(UUID::class.java, File::class.java)

            return CachingProvider().cacheManager.createCache("mlModels", mutableConfiguration)
        }

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
        ): ProducerFactory<UUID, Map<String, Any>> {
            val configurations = ImmutableMap.builder<String, Any>()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.bootstrapServers)
                .build()

            return DefaultKafkaProducerFactory(
                configurations,
                UUIDSerializer(),
                JsonSerializer(object : TypeReference<Map<String, Any>>() {}, objectMapper)
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

        @Bean
        protected fun transactionTemplate(): TransactionTemplate = Mockito.mock(TransactionTemplate::class.java)
    }
}