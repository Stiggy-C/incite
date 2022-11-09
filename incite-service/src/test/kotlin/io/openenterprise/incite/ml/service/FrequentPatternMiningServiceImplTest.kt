package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.FPGrowth
import io.openenterprise.incite.data.domain.FrequentPatternMining
import io.openenterprise.incite.data.repository.FrequentPatternMiningRepository
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.fpm.FPGrowthModel
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Before
import org.junit.Ignore
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
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.inject.Inject

@Ignore
@RunWith(SpringRunner::class)
@Import(AbstractMachineLearningServiceImplTest.Configuration::class)
class FrequentPatternMiningServiceImplTest : AbstractMachineLearningServiceImplTest() {

    @Autowired
    private lateinit var frequentPatternMiningRepository: FrequentPatternMiningRepository

    @Autowired
    private lateinit var frequentPatternMiningService: FrequentPatternMiningService

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Inject
    private lateinit var kafkaTemplate: KafkaTemplate<UUID, Map<String, Any>>

    @Before
    fun before() {
        jdbcTemplate.update("create table if not exists guest_items (id bigint primary key, items varchar)")
        jdbcTemplate.update("insert into guest_items values (1, '1 2 3 4 5') on conflict do nothing")
        jdbcTemplate.update("insert into guest_items values (2, ' 2 3 4 5') on conflict do nothing")
        jdbcTemplate.update("insert into guest_items values (3, '3 4 5') on conflict do nothing")
        jdbcTemplate.update("insert into guest_items values (4, '4 5') on conflict do nothing")
        jdbcTemplate.update("insert into guest_items values (5, '5') on conflict do nothing")
        jdbcTemplate.update("insert into guest_items values (6, '1 2 4 5') on conflict do nothing")
        jdbcTemplate.update("insert into guest_items values (7, '2 4 5 6') on conflict do nothing")
        jdbcTemplate.update("insert into guest_items values (8, '1 3 4 6') on conflict do nothing")
        jdbcTemplate.update("insert into guest_items values (9, '2 4 6') on conflict do nothing")
        jdbcTemplate.update("insert into guest_items values (10, '3 4 5 6 7') on conflict do nothing")
    }

    @Test
    fun testSetUp() {
        val algo = FrequentPatternMining.SupportedAlgorithm.FP_GROWTH.name
        val algoSpecificParams = "{\"minConfidence\": 0.75}"
        val sqlString = "select * from guest_items"
        val sinkTable = "test_set_up_frequent_pattern_mining"
        val primaryKeyColumns = "id"

        Mockito.`when`(frequentPatternMiningRepository.save(Mockito.any())).thenAnswer {
            (it.arguments[0] as FrequentPatternMining).id = UUID.randomUUID().toString()

            it.arguments[0]
        }

        val id = FrequentPatternMiningService.setUp(algo, algoSpecificParams, sqlString, sinkTable, primaryKeyColumns)

        assertNotNull(id)
    }

    @Test
    fun testTrainFpGrowthModel() {
        val algorithm = FPGrowth()
        val jdbcSource = jdbcSource("select * from guest_items")

        val frequentPatternMining = FrequentPatternMining()
        frequentPatternMining.id = UUID.randomUUID().toString()
        frequentPatternMining.algorithm = algorithm
        frequentPatternMining.sources = mutableListOf(jdbcSource)

        Mockito.`when`(frequentPatternMiningService.retrieve(frequentPatternMining.id!!))
            .thenReturn(frequentPatternMining)

        val model: FPGrowthModel = frequentPatternMiningService.train(frequentPatternMining)

        assertNotNull(model)
        assertEquals(algorithm.minConfidence, model.minConfidence, 0.0)
        assertEquals(algorithm.minSupport, model.minSupport, 0.0)
    }

    @TestConfiguration
    class Configuration {

        @Bean
        protected fun frequentPatternMiningRepository(): FrequentPatternMiningRepository =
            Mockito.mock(FrequentPatternMiningRepository::class.java)

        @Bean
        protected fun frequentPatternMiningService(
            datasetService: DatasetService,
            pipelineService: PipelineService
        ): FrequentPatternMiningService =
            FrequentPatternMiningServiceImpl(datasetService, pipelineService)
    }
}