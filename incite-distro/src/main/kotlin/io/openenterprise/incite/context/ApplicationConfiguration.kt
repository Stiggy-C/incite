package io.openenterprise.incite.context

import com.fasterxml.jackson.databind.ObjectMapper
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
    @ConditionalOnBean(IgniteCluster::class)
    @Primary
    fun datasource(@Value("\${ignite.sqlConfiguration.sqlSchemas}") schemas: Array<String>): DataSource {
        val igniteJdbcThinDataSource = IgniteJdbcThinDataSource()
        igniteJdbcThinDataSource.password = "ignite"
        igniteJdbcThinDataSource.username = "ignite"
        igniteJdbcThinDataSource.schema = schemas[0]

        igniteJdbcThinDataSource.setUrl("jdbc:ignite:thin://localhost:10800")

        return igniteJdbcThinDataSource
    }

    @Bean
    fun databaseStartupValidator(dataSource: DataSource): DatabaseStartupValidator {
        val databaseStartupValidator = DatabaseStartupValidator()
        databaseStartupValidator.setInterval(3)
        databaseStartupValidator.setValidationQuery("SET LOCK_MODE = 3")

        return databaseStartupValidator
    }

    @Bean
    fun objectMapper(): ObjectMapper {
        return ObjectMapper()
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