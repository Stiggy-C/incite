package io.openenterprise.incite.ml.service

import com.google.common.collect.Sets
import io.openenterprise.ignite.cache.query.ml.ClusteringFunction
import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.data.repository.ClusteringRepository
import io.openenterprise.incite.service.AggregateService
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
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils
import java.util.*

@RunWith(SpringJUnit4ClassRunner::class)
class ClusteringServiceImplTest {

    private var clustering: Clustering = Clustering()

    @Autowired
    private lateinit var clusteringService: ClusteringService

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var postgreSQLContainer: PostgreSQLContainer<*>

    @Before
    fun before() {
        val rdbmsDatabase = RdbmsDatabase()
        rdbmsDatabase.driverClass = postgreSQLContainer.driverClassName
        rdbmsDatabase.url = postgreSQLContainer.jdbcUrl
        rdbmsDatabase.username = postgreSQLContainer.username
        rdbmsDatabase.password = postgreSQLContainer.password

        val jdbcSource = JdbcSource()
        jdbcSource.query = "select g.id, g.age, g.sex from guest g"
        jdbcSource.rdbmsDatabase = rdbmsDatabase

        clustering.id = UUID.randomUUID().toString()
        clustering.sources = Lists.list(jdbcSource)

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

        Mockito.`when`(clusteringService.retrieve(clustering.id.toString())).thenReturn(clustering)
    }

    @Test
    fun buildBisectingKMeansModel() {
        val algorithm = BisectingKMeans()
        algorithm.featureColumns = Sets.newHashSet("age", "sex")
        algorithm.k = 4

        clustering.algorithm = algorithm

        val bisectingKMeansModel: BisectingKMeansModel = clusteringService.buildModel(clustering)

        Assert.assertNotNull(bisectingKMeansModel)
        Assert.assertTrue(bisectingKMeansModel.clusterCenters().isNotEmpty())
        Assert.assertTrue(bisectingKMeansModel.hasSummary())
    }

    @Test
    fun buildKMeansModel() {
        val algorithm = KMeans()
        algorithm.featureColumns = Sets.newHashSet("age", "sex")
        algorithm.k = 4

        clustering.algorithm = algorithm

        val kMeansModel: KMeansModel = clusteringService.buildModel(clustering)

        Assert.assertNotNull(kMeansModel)
        Assert.assertTrue(kMeansModel.clusterCenters().isNotEmpty())
        Assert.assertTrue(kMeansModel.hasSummary())
    }

    @TestConfiguration
    @ComponentScan(
        value = [
            "io.openenterprise.incite.spark.sql.service", "io.openenterprise.springframework.context"
        ]
    )
    @Import(AbstractMLServiceImplTest.Configuration::class)
    class Configuration {

        @Bean
        protected fun clusteringFunction(): ClusteringFunction = ClusteringFunction()

        @Bean
        protected fun clusteringRepository(): ClusteringRepository =
            Mockito.mock(ClusteringRepository::class.java)

        @Bean
        protected fun clusteringService(
            aggregateService: AggregateService, clusteringFunction: ClusteringFunction
        ): ClusteringService {
            return ClusteringServiceImpl(aggregateService, clusteringFunction)
        }

    }
}
