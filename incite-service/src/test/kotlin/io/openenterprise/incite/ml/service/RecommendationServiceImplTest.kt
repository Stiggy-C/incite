package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.RecommendationFunction
import io.openenterprise.incite.data.domain.AlternatingLeastSquares
import io.openenterprise.incite.data.domain.JdbcSource
import io.openenterprise.incite.data.domain.RdbmsDatabase
import io.openenterprise.incite.data.domain.Recommendation
import io.openenterprise.incite.data.repository.RecommendationRepository
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.recommendation.ALSModel
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
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.PostgreSQLContainer
import java.util.*

@RunWith(SpringJUnit4ClassRunner::class)
class RecommendationServiceImplTest {

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
        jdbcSource.query = "select r.user, r.item, r.rating from rating r"
        jdbcSource.rdbmsDatabase = rdbmsDatabase

        val algorithm = AlternatingLeastSquares()

        recommendation.id = UUID.randomUUID().toString()
        recommendation.algorithm = algorithm
        recommendation.sources = arrayListOf(jdbcSource)

        jdbcTemplate.execute("create table if not exists rating (id bigint primary key, userId bigint not null, " +
                "skuId varchar not null, rating float not null)")

        jdbcTemplate.update("insert into rating values (1, 1021324, 'a013243', 7.7)")
        jdbcTemplate.update("insert into rating values (2, 2132435, 'b113355', 6.8)")
        jdbcTemplate.update("insert into rating values (3, 5241303, 'c023364', 9.4)")
        jdbcTemplate.update("insert into rating values (4, 5241303, 'a013243', 8.0)")
        jdbcTemplate.update("insert into rating values (5, 5241303, 'b113355', 5.5)")
        jdbcTemplate.update("insert into rating values (6, 1021324, 'b113355', 7.5)")
        jdbcTemplate.update("insert into rating values (7, 1021324, 'c023364', 8.9)")
        jdbcTemplate.update("insert into rating values (8, 2041628, 'c023364', 9.9)")
    }

    @Test
    fun buildModel() {
        val alsModel: ALSModel = recommendationService.buildModel(recommendation)

        Assert.assertNotNull(alsModel)
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
        protected fun collaborativeFilteringFunction(): RecommendationFunction =
            RecommendationFunction()

        @Bean
        protected fun collaborativeFilteringRepository(): RecommendationRepository =
            Mockito.mock(RecommendationRepository::class.java)

        @Bean
        protected fun collaborativeFilteringService(
            aggregateService: AggregateService,
            datasetService: DatasetService,
            recommendationFunction: RecommendationFunction,
            transactionTemplate: TransactionTemplate
        ): RecommendationService =
            RecommendationServiceImpl(aggregateService, datasetService, recommendationFunction, transactionTemplate)
    }
}