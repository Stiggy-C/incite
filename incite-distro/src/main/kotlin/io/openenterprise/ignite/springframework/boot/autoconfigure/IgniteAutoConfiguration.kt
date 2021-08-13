package io.openenterprise.ignite.springframework.boot.autoconfigure

import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn

@Configuration
@EnableConfigurationProperties
class IgniteAutoConfiguration: org.apache.ignite.springframework.boot.autoconfigure.IgniteAutoConfiguration() {

    @Bean
    @ConditionalOnMissingBean(IgniteCluster::class)
    protected fun igniteCluster(ignite: Ignite): IgniteCluster {
        return ignite.cluster()
    }
}