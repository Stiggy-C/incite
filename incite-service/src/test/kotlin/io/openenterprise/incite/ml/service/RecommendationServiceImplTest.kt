package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.*
import io.openenterprise.incite.data.repository.RecommendationRepository
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.recommendation.ALSModel
import org.junit.Assert.assertNotNull
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
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.PostgreSQLContainer
import java.util.*

@RunWith(SpringRunner::class)
class RecommendationServiceImplTest {

    @Autowired
    private lateinit var recommendationRepository: RecommendationRepository

    @Autowired
    private lateinit var recommendationService: RecommendationService

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var postgreSQLContainer: PostgreSQLContainer<*>

    private var recommendation: Recommendation = Recommendation()

    @Before
    fun before() {
        val rdbmsDatabase = RdbmsDatabase()
        rdbmsDatabase.driverClass = postgreSQLContainer.driverClassName
        rdbmsDatabase.url = postgreSQLContainer.jdbcUrl
        rdbmsDatabase.username = postgreSQLContainer.username
        rdbmsDatabase.password = postgreSQLContainer.password

        val jdbcSource = JdbcSource()
        jdbcSource.query = "select r.\"user\", r.item, r.rating from rating r"
        jdbcSource.rdbmsDatabase = rdbmsDatabase

        val algorithm = AlternatingLeastSquares()

        recommendation.id = UUID.randomUUID().toString()
        recommendation.algorithm = algorithm
        recommendation.sources = arrayListOf(jdbcSource)

        jdbcTemplate.execute("create table if not exists rating (id bigint primary key, \"user\" bigint not null, " +
                "item bigint not null, rating float not null)")

        jdbcTemplate.update("insert into rating values (1, 1021324, 13243, 7.7) on conflict do nothing")
        jdbcTemplate.update("insert into rating values (2, 2132435, 113355, 6.8) on conflict do nothing")
        jdbcTemplate.update("insert into rating values (3, 5241303, 23364, 9.4) on conflict do nothing")
        jdbcTemplate.update("insert into rating values (4, 5241303, 13243, 8.0) on conflict do nothing")
        jdbcTemplate.update("insert into rating values (5, 5241303, 113355, 5.5) on conflict do nothing")
        jdbcTemplate.update("insert into rating values (6, 1021324, 113355, 7.5) on conflict do nothing")
        jdbcTemplate.update("insert into rating values (7, 1021324, 23364, 8.9) on conflict do nothing")
        jdbcTemplate.update("insert into rating values (8, 2041628, 23364, 9.9) on conflict do nothing")
    }

    @Test
    fun testSetUp() {
        val algo = "AlternatingLeastSquares"
        val algoSpecificParams = "{\"itemColumn\": \"item\", \"maxIterations\": 10, \"userColumn\": \"user\"}"
        val sourceSql = "select r.\"user\", r.item, r.rating from rating r"
        val sinkTable = "test_set_up_recommendation"
        val primaryKeyColumn = "id"

        Mockito.`when`(recommendationRepository.save(Mockito.any())).thenAnswer {
            (it.arguments[0] as Recommendation).id = UUID.randomUUID().toString()

            it.arguments[0]
        }
        val uuid = RecommendationService.setUp(algo, algoSpecificParams, sourceSql, sinkTable, primaryKeyColumn)

        assertNotNull(uuid)
    }

    @Test
    fun testTrainAlternatingLeastSquares() {
        val alsModel: ALSModel = recommendationService.train(recommendation)

        assertNotNull(alsModel)
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
        protected fun recommendationRepository(): RecommendationRepository =
            Mockito.mock(RecommendationRepository::class.java)

        @Bean
        protected fun recommendationService(
            aggregateService: PipelineService,
            datasetService: DatasetService,
            transactionTemplate: TransactionTemplate
        ): RecommendationService =
            RecommendationServiceImpl(aggregateService, datasetService, transactionTemplate)
    }
}