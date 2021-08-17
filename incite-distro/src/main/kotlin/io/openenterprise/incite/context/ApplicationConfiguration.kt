package io.openenterprise.incite.context

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import io.openenterprise.springframework.jdbc.support.IgniteStartupValidator
import org.apache.ignite.IgniteCluster
import org.apache.ignite.IgniteJdbcThinDataSource
import org.apache.ignite.cache.CachingProvider
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.jdbc.DatabaseDriver
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.support.DatabaseStartupValidator
import javax.cache.CacheManager
import javax.sql.DataSource

@Configuration
class ApplicationConfiguration {

    @Bean
    fun cachingProvider(): CacheManager {
        return CachingProvider().cacheManager
    }

    @Bean
    fun objectMapper(): ObjectMapper {
        return ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .findAndRegisterModules()
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
    }

    @Bean
    fun sparkSession(
        @Value("\${spark.appName}") appName: String,
        @Value("\${spark.masterUrl}") masterUrl: String
    ): SparkSession {
        return SparkSession.builder()
            .appName("incite")
            .master(masterUrl)
            .orCreate
    }
}