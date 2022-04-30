package io.openenterprise.incite.ml.service

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Sets
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.data.repository.ClusteringRepository
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.clustering.KMeansModel
import org.assertj.core.util.Lists
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils
import java.util.*
import javax.inject.Inject

@RunWith(SpringRunner::class)
class ClusteringServiceImplTest {

    @Autowired
    private lateinit var clusteringRepository: ClusteringRepository

    @Autowired
    private lateinit var clusteringService: ClusteringService

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var kafkaContainer: KafkaContainer

    @Inject
    private lateinit var kafkaTemplate: KafkaTemplate<UUID, Map<String, Any>>

    @Autowired
    private lateinit var postgreSQLContainer: PostgreSQLContainer<*>

    @Before
    fun before() {
        jdbcTemplate.update(
            "create table if not exists guest (id bigint primary key, membership_number varchar, age smallint, " +
                    "sex smallint, created_date_time timestamp with time zone, last_login_date_time timestamp with time zone)"
        )
        jdbcTemplate.update("insert into guest values (1, '${RandomStringUtils.randomNumeric(9)}', 35, 0, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (2, '${RandomStringUtils.randomNumeric(9)}', 18, 1, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (3, '${RandomStringUtils.randomNumeric(9)}', 20, 1, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (4, '${RandomStringUtils.randomNumeric(9)}', 40, 1, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (5, '${RandomStringUtils.randomNumeric(9)}', 65, 1, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (6, '${RandomStringUtils.randomNumeric(9)}', 33, 0, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (7, '${RandomStringUtils.randomNumeric(9)}', 16, 0, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (8, '${RandomStringUtils.randomNumeric(9)}', 25, 0, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (9, '${RandomStringUtils.randomNumeric(9)}', 9, 1, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (10, '${RandomStringUtils.randomNumeric(9)}', 46, 1, now(), now()) on conflict do nothing")
    }

    @Test
    fun testSetUp() {
        val algo = Clustering.Algorithm.Supported.BisectingKMeans.name
        val algoSpecificParams = "{\"featureColumns\": [\"age\", \"sex\"], \"k\": 5, \"maxIterations\": 10}"
        val sqlString = "select g.id, g.age, g.sex from guest g"
        val sinkTable = "test_set_up_clustering"
        val primaryKeyColumn = "id"

        Mockito.`when`(clusteringRepository.save(Mockito.any())).thenAnswer {
            (it.arguments[0] as Clustering).id = UUID.randomUUID().toString()

            it.arguments[0]
        }

        val clusteringId: UUID =
            ClusteringService.setUp(algo, algoSpecificParams, sqlString, sinkTable, primaryKeyColumn)

        Assert.assertNotNull(clusteringId)
    }

    @Test
    fun testTrainBisectingKMeansModel() {
        val jdbcSource = jdbcSource()

        val algorithm = BisectingKMeans()
        algorithm.featureColumns = Sets.newHashSet("age", "sex")
        algorithm.k = 4

        val clustering = Clustering()
        clustering.algorithm = algorithm
        clustering.id = UUID.randomUUID().toString()
        clustering.sources = Lists.list(jdbcSource)

        givenClusteringIdReturnClusteringEntity(clustering)

        val bisectingKMeansModel: BisectingKMeansModel = clusteringService.train(clustering)

        Assert.assertNotNull(bisectingKMeansModel)
        Assert.assertTrue(bisectingKMeansModel.clusterCenters().isNotEmpty())
        Assert.assertTrue(bisectingKMeansModel.hasSummary())
    }

    @Test
    fun testTrainKMeansModel() {
        val jdbcSource = jdbcSource()

        val algorithm = KMeans()
        algorithm.featureColumns = Sets.newHashSet("age", "sex")
        algorithm.k = 4

        val clustering = Clustering()
        clustering.algorithm = algorithm
        clustering.id = UUID.randomUUID().toString()
        clustering.sources = Lists.list(jdbcSource)

        givenClusteringIdReturnClusteringEntity(clustering)

        val kMeansModel: KMeansModel = clusteringService.train(clustering)

        Assert.assertNotNull(kMeansModel)
        Assert.assertTrue(kMeansModel.clusterCenters().isNotEmpty())
        Assert.assertTrue(kMeansModel.hasSummary())
    }

    @Test
    fun testTrainKMeansModelFromJointDatasets() {
        val topic = "testBuildKMeansModelFromJointDatasets"

        val jdbcSource = jdbcSource()
        val kafkaSource =
            kafkaSource(
                topic,
                Field("guest_id", "cast(#field as bigint) as guest_id"),
                Field("average_spending", "cast(#field as double) as average_spending")
            )

        val algorithm = BisectingKMeans()
        algorithm.featureColumns = Sets.newHashSet("age", "sex", "average_spending")
        algorithm.k = 4

        val join = Join()
        join.leftColumn = "guest_id"
        join.rightColumn = "id"
        join.rightIndex = 1
        join.type = Join.Type.INNER

        val clustering = Clustering()
        clustering.algorithm = algorithm
        clustering.id = UUID.randomUUID().toString()
        clustering.joins = Lists.list(join)
        clustering.sources = Lists.list(kafkaSource, jdbcSource)

        val message0 = ImmutableMap.of("guest_id", 1, "average_spending", 101.0)
        val message1 = ImmutableMap.of("guest_id", 2, "average_spending", 57.2)
        val message2 = ImmutableMap.of("guest_id", 3, "average_spending", 1000.7)
        val message3 = ImmutableMap.of("guest_id", 4, "average_spending", 211.4)
        val message4 = ImmutableMap.of("guest_id", 5, "average_spending", 91.3)
        val message5 = ImmutableMap.of("guest_id", 6, "average_spending", 891.1)

        kafkaTemplate.send(topic, UUID.randomUUID(), message0).get()
        kafkaTemplate.send(topic, UUID.randomUUID(), message1).get()
        kafkaTemplate.send(topic, UUID.randomUUID(), message2).get()
        kafkaTemplate.send(topic, UUID.randomUUID(), message3).get()
        kafkaTemplate.send(topic, UUID.randomUUID(), message4).get()
        kafkaTemplate.send(topic, UUID.randomUUID(), message5).get()

        givenClusteringIdReturnClusteringEntity(clustering)

        val bisectingKMeansModel: BisectingKMeansModel = clusteringService.train(clustering)

        Assert.assertNotNull(bisectingKMeansModel)
        Assert.assertTrue(bisectingKMeansModel.clusterCenters().isNotEmpty())
        Assert.assertTrue(bisectingKMeansModel.hasSummary())
    }

    private fun givenClusteringIdReturnClusteringEntity(clustering: Clustering) {
        Mockito.`when`(clusteringService.retrieve(clustering.id.toString())).thenReturn(clustering)
    }

    private fun jdbcSource(): JdbcSource {
        val rdbmsDatabase = RdbmsDatabase()
        rdbmsDatabase.driverClass = postgreSQLContainer.driverClassName
        rdbmsDatabase.url = postgreSQLContainer.jdbcUrl
        rdbmsDatabase.username = postgreSQLContainer.username
        rdbmsDatabase.password = postgreSQLContainer.password

        val jdbcSource = JdbcSource()
        jdbcSource.query = "select g.id, g.age, g.sex from guest g"
        jdbcSource.rdbmsDatabase = rdbmsDatabase
        return jdbcSource
    }

    private fun kafkaSource(topic: String, vararg fields: Field): KafkaSource {
        val kafkaCluster = KafkaCluster()
        kafkaCluster.servers = kafkaContainer.bootstrapServers

        val kafkaSource = KafkaSource()
        kafkaSource.fields = Sets.newHashSet(*fields)
        kafkaSource.kafkaCluster = kafkaCluster
        kafkaSource.startingOffset = "earliest"
        kafkaSource.streamingRead = false
        kafkaSource.topic = topic

        return kafkaSource
    }

    @TestConfiguration
    @ComponentScan(
        value = [
            "io.openenterprise.incite.spark.sql.service", "io.openenterprise.springframework.context"
        ]
    )
    @Import(AbstractMachineLearningServiceImplTest.Configuration::class)
    class Configuration {

        @Bean
        protected fun clusteringRepository(): ClusteringRepository =
            Mockito.mock(ClusteringRepository::class.java)

        @Bean
        protected fun clusteringService(
            aggregateService: AggregateService,
            datasetService: DatasetService,
            transactionTemplate: TransactionTemplate
        ): ClusteringService {
            return ClusteringServiceImpl(aggregateService, datasetService, transactionTemplate)
        }
    }
}
