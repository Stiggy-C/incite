package io.openenterprise.incite.ml.service

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.google.common.collect.Sets
import io.openenterprise.ignite.cache.query.ml.ClassificationFunction
import io.openenterprise.ignite.cache.query.ml.ClusteringFunction
import io.openenterprise.ignite.spark.IgniteContext
import io.openenterprise.incite.data.domain.Classification
import io.openenterprise.incite.data.domain.JdbcSource
import io.openenterprise.incite.data.domain.LogisticRegression
import io.openenterprise.incite.data.domain.RdbmsDatabase
import io.openenterprise.incite.data.repository.AggregateRepository
import io.openenterprise.incite.data.repository.ClassificationRepository
import io.openenterprise.incite.data.repository.ClusteringRepository
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
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
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.assertj.core.util.Lists
import org.junit.Test

import org.junit.Assert.*
import org.junit.Before
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.postgresql.ds.PGSimpleDataSource
import org.springframework.beans.factory.annotation.Autowired
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
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils
import java.io.File
import java.util.*
import javax.cache.Cache
import javax.cache.configuration.MutableConfiguration
import javax.sql.DataSource

@RunWith(SpringJUnit4ClassRunner::class)
class ClassificationServiceImplTest {

    private var classification = Classification()

    @Autowired
    private lateinit var classificationService: ClassificationService

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
        jdbcSource.query = "select g.id, g.age, g.sex, g.result from guest g"
        jdbcSource.rdbmsDatabase = rdbmsDatabase

        classification.id = UUID.randomUUID().toString()
        classification.sources = Lists.list(jdbcSource)

        jdbcTemplate.update(
            "create table if not exists guest (id bigint primary key, membership_number varchar, age smallint, " +
                    "sex smallint, result smallint, created_date_time timestamp with time zone, last_login_date_time timestamp with time zone)"
        )
        jdbcTemplate.update("insert into guest values (1, '${RandomStringUtils.randomNumeric(9)}', 35, 0, 3, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (2, '${RandomStringUtils.randomNumeric(9)}', 18, 1, 2, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (3, '${RandomStringUtils.randomNumeric(9)}', 20, 1, 2, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (4, '${RandomStringUtils.randomNumeric(9)}', 40, 1, 4, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (5, '${RandomStringUtils.randomNumeric(9)}', 65, 1, 5, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (6, '${RandomStringUtils.randomNumeric(9)}', 33, 0, 3, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (7, '${RandomStringUtils.randomNumeric(9)}', 16, 0, 1, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (8, '${RandomStringUtils.randomNumeric(9)}', 25, 0, 2, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (9, '${RandomStringUtils.randomNumeric(9)}', 9, 1, 1, now(), now()) on conflict do nothing")
        jdbcTemplate.update("insert into guest values (10, '${RandomStringUtils.randomNumeric(9)}', 46, 1, 5, now(), now()) on conflict do nothing")

        Mockito.`when`(classificationService.retrieve(classification.id.toString())).thenReturn(classification)
    }

    @Test
    fun buildModel() {
        val algorithm = LogisticRegression()
        algorithm.labelColumn = "result"
        algorithm.featureColumns = Sets.newHashSet("age", "sex")

        classification.algorithm = algorithm

        val logisticRegressionModel: LogisticRegressionModel = classificationService.buildModel(classification)

        assertNotNull(logisticRegressionModel)
        assertTrue(logisticRegressionModel.hasSummary())
    }

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
        protected fun classificationFunction(): ClassificationFunction = ClassificationFunction()

        @Bean
        protected fun classificationRepository(): ClassificationRepository =
            Mockito.mock(ClassificationRepository::class.java)

        @Bean
        protected fun classificationService(
            aggregateService: AggregateService,
            classificationFunction: ClassificationFunction
        ): ClassificationService =
            ClassificationServiceImpl(aggregateService, classificationFunction)

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