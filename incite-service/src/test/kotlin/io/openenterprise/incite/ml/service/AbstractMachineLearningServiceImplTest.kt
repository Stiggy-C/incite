package io.openenterprise.incite.ml.service

import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Sets
import io.openenterprise.incite.TestUtils
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.spark.service.DatasetServiceImplTest
import io.openenterprise.incite.spark.sql.streaming.StreamingQueryListener
import org.apache.ignite.cache.CachingProvider
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.UUIDSerializer
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.DependsOn
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import org.testcontainers.Testcontainers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.util.*
import javax.cache.Cache
import javax.cache.configuration.MutableConfiguration

abstract class AbstractMachineLearningServiceImplTest {

    @Autowired
    protected lateinit var kafkaContainer: KafkaContainer

    @Autowired
    protected lateinit var postgreSQLContainer: PostgreSQLContainer<*>

    protected fun jdbcSource(query: String): JdbcSource {
        val rdbmsDatabase = rdbmsDatabase()

        val jdbcSource = JdbcSource()
        jdbcSource.query = query
        jdbcSource.rdbmsDatabase = rdbmsDatabase

        return jdbcSource
    }

    protected fun kafkaSource(topic: String, vararg fields: Field): KafkaSource {
        val kafkaCluster = kafkaCluster()

        val kafkaSource = KafkaSource()
        kafkaSource.fields = Sets.newHashSet(*fields)
        kafkaSource.kafkaCluster = kafkaCluster
        kafkaSource.startingOffset = KafkaSource.Offset.Earliest
        kafkaSource.streamingRead = false
        kafkaSource.topic = topic

        return kafkaSource
    }

    protected fun kafkaCluster(): KafkaCluster {
        val kafkaCluster = KafkaCluster()
        kafkaCluster.servers = TestUtils.manipulateKafkaBootstrapServers(kafkaContainer)


        return kafkaCluster
    }

    protected fun rdbmsDatabase(): RdbmsDatabase {
        val rdbmsDatabase = RdbmsDatabase()
        rdbmsDatabase.driverClass = postgreSQLContainer.driverClassName
        rdbmsDatabase.url = "jdbc:postgresql://host.testcontainers.internal:5432/${postgreSQLContainer.databaseName}"
        rdbmsDatabase.username = postgreSQLContainer.username
        rdbmsDatabase.password = postgreSQLContainer.password

        return rdbmsDatabase
    }

    @TestConfiguration
    @Import(io.openenterprise.incite.TestConfiguration::class)
    class Configuration {

        @Bean
        protected fun amazonS3(localStackContainer: LocalStackContainer): AmazonS3 = AmazonS3ClientBuilder.standard()
            .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
            .withCredentials(
                AWSStaticCredentialsProvider(
                    BasicAWSCredentials(localStackContainer.accessKey, localStackContainer.secretKey)
                )
            )
            .withEndpointConfiguration(
                AwsClientBuilder.EndpointConfiguration(
                    "host.testcontainers.internal:4566",
                    localStackContainer.region
                )
            )
            .withPathStyleAccessEnabled(true)
            .build()

        @Bean
        protected fun kafkaTemplate(producerFactory: ProducerFactory<UUID, Map<String, Any>>): KafkaTemplate<UUID, Map<String, Any>> =
            KafkaTemplate(producerFactory)

        @Bean
        @Order(Ordered.HIGHEST_PRECEDENCE + 1)
        protected fun localStackContainer(network: Network): LocalStackContainer {
            val localStackContainer = LocalStackContainer(DockerImageName.parse("localstack/localstack:1.2.0"))
                .withExposedPorts(4566)
                .withNetwork(network)
                .withNetworkAliases("aws")
                .withNetworkMode("host")
                .withServices(LocalStackContainer.Service.S3)

            localStackContainer.portBindings = listOf("4566:4566")

            localStackContainer.start().apply { Testcontainers.exposeHostPorts(4566) }

            return localStackContainer
        }

        @Bean("mlModelsCache")
        fun mlModelsCache(): Cache<UUID, File> {
            val mutableConfiguration: MutableConfiguration<UUID, File> =
                MutableConfiguration<UUID, File>()
            mutableConfiguration.setTypes(UUID::class.java, File::class.java)

            return CachingProvider().cacheManager.createCache("mlModels", mutableConfiguration)
        }

        @Bean
        protected fun producerFactory(
            kafkaContainer: io.openenterprise.testcontainers.containers.KafkaContainer, objectMapper: ObjectMapper
        ): ProducerFactory<UUID, Map<String, Any>> {
            val bootstrapServers = TestUtils.manipulateKafkaBootstrapServers(kafkaContainer)

            val configurations = ImmutableMap.builder<String, Any>()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers).build()

            return DefaultKafkaProducerFactory(
                configurations,
                UUIDSerializer(),
                JsonSerializer(object : TypeReference<Map<String, Any>>() {}, objectMapper)
            )
        }

        @Bean
        @ConditionalOnBean(LocalStackContainer::class)
        @DependsOn("sparkWorkerContainer")
        @Primary
        protected fun sparkSession(
            @Qualifier("sparkMasterContainer") genericContainer: GenericContainer<*>,
            localStackContainer: LocalStackContainer,
            streamingQueryListener: StreamingQueryListener
        ): SparkSession {
            /* val userDir = System.getProperty("user.dir")
            val inciteIgniteJar = "${userDir}/../incite-ignite/target/incite-ignite-0.0.1-SNAPSHOT.jar"
            val inciteServiceJar = "${userDir}/../incite-service/target/incite-service-0.0.1-SNAPSHOT.jar"

            val userHome = System.getProperty("user.home")
            val apacheCommonsPool2Jar = "${userHome}/.m2/repository/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
            val apacheKafkaClientJar = "${userHome}/.m2/repository/org/apache/kafka/kafka-clients/3.1.2/kafka-clients-3.1.2.jar"
            val apacheSparkSqlKafkaJar = "${userHome}/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar"
            val apacheSparkTokenProviderJar = "${userHome}/.m2/repository/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.0/spark-token-provider-kafka-0-10_2.12-3.3.0.jar"*/

            val sparkSession = SparkSession.builder()
                .appName(DatasetServiceImplTest::class.java.simpleName)
                .master("spark://127.0.0.1:7077")
                .config("spark.executor.memory", "512m")
                .config("spark.executor.memoryOverhead", "512m")
                .config("spark.hadoop.fs.s3a.access.key", localStackContainer.accessKey)
                .config("spark.hadoop.fs.s3a.secret.key", localStackContainer.secretKey)
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", false)
                .config("spark.hadoop.fs.s3a.endpoint", "host.testcontainers.internal:4566")
                .config("spark.hadoop.fs.s3a.path.style.access", true)
                /*.config("spark.jars", "$apacheCommonsPool2Jar, $apacheKafkaClientJar, $apacheSparkSqlKafkaJar," +
                        " $apacheSparkTokenProviderJar, $inciteIgniteJar, $inciteServiceJar")*/
                .config("spark.memory.offHeap.enabled", "true")
                .config("spark.memory.offHeap.size", "512m")
                .config("spark.sql.streaming.schemaInference", "true")
                .config("spark.submit.deployMode", "client")
                .orCreate

            sparkSession.streams().addListener(streamingQueryListener)

            return sparkSession
        }
    }
}