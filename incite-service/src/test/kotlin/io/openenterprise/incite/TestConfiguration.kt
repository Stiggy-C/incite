package io.openenterprise.incite

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.github.dockerjava.api.model.ContainerNetwork
import io.openenterprise.incite.data.repository.AggregateRepository
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.incite.service.PipelineServiceImpl
import io.openenterprise.incite.spark.service.DatasetServiceImplTest
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.incite.spark.sql.service.DatasetServiceImpl
import io.openenterprise.incite.spark.sql.streaming.StreamingQueryListener
import io.openenterprise.testcontainers.containers.KafkaContainer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import org.apache.ignite.Ignition
import org.apache.ignite.cluster.ClusterState
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.configuration.SqlConfiguration
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito
import org.postgresql.ds.PGSimpleDataSource
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.DependsOn
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.Testcontainers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import javax.sql.DataSource

@TestConfiguration
@ComponentScan(
    value = ["io.openenterprise.incite.spark.sql.service", "io.openenterprise.incite.spark.sql.streaming", "io.openenterprise.springframework.context"]
)
class TestConfiguration {

    @Bean
    protected fun aggregateRepository(): AggregateRepository = Mockito.mock(AggregateRepository::class.java)


    @Bean
    protected fun coroutineScope(): CoroutineScope = CoroutineScope(Dispatchers.Default)

    @Bean
    protected fun datasetService(
        coroutineScope: CoroutineScope,
        @Value("\${io.openenterprise.incite.spark.checkpoint-location-root:./spark-checkpoints}") sparkCheckpointLocation: String,
        sparkSession: SparkSession,
        spelExpressionParser: SpelExpressionParser
    ): DatasetService =
        DatasetServiceImpl(coroutineScope, sparkCheckpointLocation, sparkSession, spelExpressionParser)

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
        val indexingQueryEngineConfiguration = IndexingQueryEngineConfiguration()
        indexingQueryEngineConfiguration.isDefault = true

        val sqlConfiguration = SqlConfiguration()
        sqlConfiguration.setQueryEnginesConfiguration(indexingQueryEngineConfiguration)

        val igniteConfiguration = IgniteConfiguration()
        igniteConfiguration.igniteInstanceName = this::class.java.simpleName
        igniteConfiguration.sqlConfiguration = sqlConfiguration

        return Ignition.getOrStart(igniteConfiguration).apply { Testcontainers.exposeHostPorts(10800) }
    }

    @Bean
    protected fun igniteCluster(ignite: Ignite): IgniteCluster = ignite.cluster()
        .apply { this.state(ClusterState.ACTIVE) }

    @Bean
    protected fun jdbcTemplate(datasource: DataSource): JdbcTemplate = JdbcTemplate(datasource)

    @Bean
    protected fun kafkaContainer(network: Network): KafkaContainer {
        val kafkaContainer = KafkaContainer()

        kafkaContainer
            .withExposedPorts(9093)
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withNetworkMode("host")

        kafkaContainer.isHostAccessible = true
        kafkaContainer.portBindings = listOf("9093:9093")

        kafkaContainer.start().apply { Testcontainers.exposeHostPorts(9093) }

        return kafkaContainer
    }

    @Bean
    protected fun network(): Network = Network.newNetwork()

    @Bean
    fun objectMapper(): ObjectMapper = ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).findAndRegisterModules()
        .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)

    @Bean
    protected fun pipelineService(datasetService: DatasetService, ignite: Ignite): PipelineService =
        PipelineServiceImpl(datasetService, ignite)

    @Bean
    protected fun postgreSQLContainer(network: Network): PostgreSQLContainer<*> {
        val postgreSQLContainer: PostgreSQLContainer<*> =
            PostgreSQLContainer<PostgreSQLContainer<*>>("postgres:14.5")
                .withExposedPorts(5432)
                .withNetwork(network)
                .withNetworkAliases("postgre")
                .withNetworkMode("host")
                .withPassword("test_password")
                .withUsername("test_user")
        postgreSQLContainer.portBindings = listOf("5432:5432")

        postgreSQLContainer.start().apply { Testcontainers.exposeHostPorts(5432) }

        return postgreSQLContainer
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @Qualifier("sparkMasterContainer")
    protected fun sparkMasterContainer(network: Network): GenericContainer<*> {
        val dockerImage = DockerImageName.parse("incite/spark:3.3.1")
        val genericContainer = GenericContainer(dockerImage)
            .waitingFor(Wait.forLogMessage(".*Master: I have been elected leader! New state: ALIVE.*", 1))
            .withCommand("/opt/bitnami/spark/sbin/start-master.sh")
            .withExposedPorts(4044, 7077, 8080)
            .withNetwork(network)
            .withNetworkAliases("spark-master")
            .withNetworkMode("host")
        genericContainer.portBindings = listOf("4044:4044", "7077:7077", "8080:8080")

        genericContainer.start()

        return genericContainer
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @Qualifier("sparkWorkerContainer")
    protected fun sparkWorkerContainer(
        network: Network,
        @Qualifier("sparkMasterContainer") sparkMasterContainer: GenericContainer<*>
    ): GenericContainer<*> {
        val sparkMasterContainerContainerNetwork: ContainerNetwork =
            sparkMasterContainer.containerInfo.networkSettings.networks[(network as Network.NetworkImpl).name/*"bridge"*/]
                ?: throw IllegalStateException()
        val sparkMasterIpAddress = sparkMasterContainerContainerNetwork.ipAddress

        val dockerImage = DockerImageName.parse("incite/spark:3.3.1")
        val genericContainer = GenericContainer(dockerImage)
            .waitingFor(Wait.forLogMessage(".*Worker: Successfully registered with master.*", 1))
            .withAccessToHost(true)
            .withCommand("/opt/bitnami/spark/sbin/start-worker.sh spark://$sparkMasterIpAddress:7077")
            .withExposedPorts(8081)
            .withNetwork(network)
            .withNetworkAliases("spark-worker")
            .withNetworkMode("host")
        genericContainer.portBindings = listOf("8081:8081")

        genericContainer.start()

        return genericContainer
    }

    @Bean
    @ConditionalOnMissingBean(SparkSession::class)
    @DependsOn("sparkWorkerContainer")
    protected fun sparkSession(
        @Qualifier("sparkMasterContainer") genericContainer: GenericContainer<*>,
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

    @Bean
    protected fun spelExpressionParser(): SpelExpressionParser = SpelExpressionParser()

    @Bean
    protected fun transactionTemplate(): TransactionTemplate = Mockito.mock(TransactionTemplate::class.java)
}