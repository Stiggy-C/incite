package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.RecommendationFunction
import io.openenterprise.incite.data.repository.RecommendationRepository
import io.openenterprise.incite.service.AggregateService
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Import
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

@RunWith(SpringJUnit4ClassRunner::class)
class RecommendationServiceImplTest {

    @Test
    fun recommendForAllUsers() {
    }

    @Test
    fun recommendForUsersSubset() {
    }

    @Test
    fun buildModel() {
    }

    @Test
    fun predict() {
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
            recommendationFunction: RecommendationFunction
        ): RecommendationService =
            RecommendationServiceImpl(aggregateService, recommendationFunction)
    }
}