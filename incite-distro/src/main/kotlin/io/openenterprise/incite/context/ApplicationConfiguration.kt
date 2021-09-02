package io.openenterprise.incite.context

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import org.apache.ignite.IgniteMessaging
import org.apache.ignite.cache.CachingProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionManager
import org.springframework.transaction.support.TransactionTemplate
import javax.cache.CacheManager

@Configuration
class ApplicationConfiguration {

    @Bean
    fun cachingProvider(): CacheManager {
        return CachingProvider().cacheManager
    }

    @Bean
    fun igniteMessaging(ignite: Ignite, igniteCluster: IgniteCluster): IgniteMessaging {
        val clusterGroup = igniteCluster.forPredicate { node -> !node.isClient }.forPredicate { node -> !node.isDaemon }

        return ignite.message(clusterGroup)
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