package io.openenterprise.incite.context

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import io.openenterprise.ignite.spark.IgniteContext
import io.openenterprise.springframework.context.ApplicationContextUtils
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import org.apache.ignite.IgniteMessaging
import org.apache.ignite.cache.CachingProvider
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.web.servlet.config.annotation.EnableWebMvc
import javax.cache.CacheManager

@Configuration
@ComponentScan(basePackages = ["io.openenterprise.springframework.context"])
@EnableWebMvc
class ApplicationConfiguration {

    @Bean
    fun cachingProvider(): CacheManager {
        return CachingProvider().cacheManager
    }

    @Bean
    fun coroutineScope(): CoroutineScope {
        return CoroutineScope(Dispatchers.Default)
    }

    @Bean
    fun igniteContext(applicationContextUtils: ApplicationContextUtils, sparkSession: SparkSession): IgniteContext {
        return IgniteContext(sparkSession.sparkContext())
    }

    @Bean
    fun igniteMessaging(ignite: Ignite, igniteCluster: IgniteCluster): IgniteMessaging {
        val clusterGroup = igniteCluster.forPredicate { node -> !node.isClient }.forPredicate { node -> !node.isDaemon }

        return ignite.message(clusterGroup)
    }

    @Bean
    @Primary
    fun igniteSparkSession(igniteContext: IgniteContext, sparkSession: SparkSession): SparkSession {
        val igniteSparkSession = IgniteSparkSession(igniteContext, sparkSession)

        try {
            return igniteSparkSession
        } finally {
            IgniteSparkSession.setActiveSession(igniteSparkSession)
        }
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
    fun transactionTemplate(platformTransactionManager: PlatformTransactionManager): TransactionTemplate {
        return TransactionTemplate(platformTransactionManager)
    }

    @Bean
    fun xmlMapper(): XmlMapper {
        return XmlMapper.builder().findAndAddModules().build()
    }

    @Bean
    fun yamlMapper(): YAMLMapper {
        return YAMLMapper.builder().findAndAddModules().build()
    }
}