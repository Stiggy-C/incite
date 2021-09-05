package io.openenterprise.incite.context

import org.apache.camel.CamelContext
import org.apache.camel.processor.idempotent.jdbc.JdbcOrphanLockAwareIdempotentRepository
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
@ComponentScan("io.openenterprise.camel.dsl.yaml")
class CamelConfiguration {

    @Bean
    @ConditionalOnBean(CamelContext::class)
    fun jdbcOrphanLockAwareIdempotentRepository(camelContext: CamelContext): JdbcOrphanLockAwareIdempotentRepository {
        val jdbcOrphanLockAwareIdempotentRepository = JdbcOrphanLockAwareIdempotentRepository(camelContext)
        jdbcOrphanLockAwareIdempotentRepository.lockMaxAgeMillis = Duration.ofMinutes(5).toMillis()
        jdbcOrphanLockAwareIdempotentRepository.lockKeepAliveIntervalMillis = Duration.ofSeconds(15).toMillis()

        return jdbcOrphanLockAwareIdempotentRepository
    }
}