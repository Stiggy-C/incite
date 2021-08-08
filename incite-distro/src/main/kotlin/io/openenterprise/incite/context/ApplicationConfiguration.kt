package io.openenterprise.incite.context

import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ApplicationConfiguration {

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