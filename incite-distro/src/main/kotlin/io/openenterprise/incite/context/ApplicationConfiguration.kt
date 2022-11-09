package io.openenterprise.incite.context

import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
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
import org.springframework.beans.factory.annotation.Value
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

    @Value("\${spark.hadoop.fs.s3a.endpoint}")
    protected var amazonS3Endpoint: String? = null

    @Value("\${spark.hadoop.fs.s3a.access.key}")
    protected var amazonS3AccessKey: String? = null

    @Value("\${spark.hadoop.fs.s3a.secret.key}")
    protected var amazonS3SecretKey: String? = null

    @Value("\${incite.aws.s3.region:ap-northeast-1}")
    protected lateinit var amazonS3Region: String

    @Value("\${spark.hadoop.fs.s3a.path.style.access:false}")
    protected var amazonS3UsesPathStyleAccess: Boolean = false

    @Value("\${spark.hadoop.fs.s3a.connection.ssl.enabled:true}")
    protected var amazonS3UsesSSLConnection: Boolean = true

    @Bean
    protected fun amazonS3(): AmazonS3 {
        var amazonS3ClientBuilder = AmazonS3ClientBuilder.standard()

        if (amazonS3AccessKey != null && amazonS3SecretKey != null) {
            amazonS3ClientBuilder = amazonS3ClientBuilder.withCredentials(
                AWSStaticCredentialsProvider(
                    BasicAWSCredentials(
                        amazonS3AccessKey,
                        amazonS3SecretKey
                    )
                )
            )
        }

        if (amazonS3Endpoint != null) {
            amazonS3ClientBuilder = amazonS3ClientBuilder.withEndpointConfiguration(
                AwsClientBuilder.EndpointConfiguration(amazonS3Endpoint, amazonS3Region)
            )
        }

        if (!amazonS3UsesPathStyleAccess) {
            amazonS3ClientBuilder = amazonS3ClientBuilder.withPathStyleAccessEnabled(true)
        }

        if (!amazonS3UsesSSLConnection) {
            amazonS3ClientBuilder = amazonS3ClientBuilder.withClientConfiguration(
                ClientConfiguration().withProtocol(Protocol.HTTP)
            )
        }

        return amazonS3ClientBuilder.build()
    }

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