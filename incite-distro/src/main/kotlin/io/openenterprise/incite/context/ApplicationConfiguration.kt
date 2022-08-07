package io.openenterprise.incite.context

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.google.common.collect.Maps
import io.openenterprise.incite.PipelineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import org.apache.ignite.IgniteMessaging
import org.apache.ignite.cache.CachingProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.web.servlet.config.annotation.EnableWebMvc
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Lock
import javax.cache.CacheManager

@Configuration
@ComponentScan("io.openenterprise.springframework.context")
@EnableWebMvc
class ApplicationConfiguration {

    @Bean
    protected fun cacheManager(cachingProvider: CachingProvider): CacheManager = cachingProvider.cacheManager

    @Bean
    protected fun cachingProvider(): CachingProvider = CachingProvider()

    @Bean
    protected fun coroutineScope(): CoroutineScope = CoroutineScope(Dispatchers.Default)

    @Bean
    protected fun objectMapper(): ObjectMapper = ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .findAndRegisterModules()
        .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)

    @Bean
    protected fun spelExpressionParser(): SpelExpressionParser = SpelExpressionParser()

    @Bean
    protected fun transactionTemplate(platformTransactionManager: PlatformTransactionManager): TransactionTemplate =
        TransactionTemplate(platformTransactionManager)

    @Bean
    protected fun xmlMapper(): XmlMapper = XmlMapper.builder().findAndAddModules().build()

    @Bean
    protected fun yamlMapper(): YAMLMapper = YAMLMapper.builder().findAndAddModules().build()
}