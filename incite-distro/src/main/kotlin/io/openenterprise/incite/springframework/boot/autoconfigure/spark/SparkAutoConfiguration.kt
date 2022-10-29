package io.openenterprise.incite.springframework.boot.autoconfigure.spark

import io.openenterprise.incite.spark.sql.streaming.StreamingQueryListener
import io.openenterprise.springframework.boot.autoconfigure.spark.SparkProperties
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary

@Configuration
@ComponentScan("io.openenterprise.incite.spark.sql.streaming")
@ConditionalOnClass(SparkContext::class)
@EnableConfigurationProperties(SparkProperties::class)
class SparkAutoConfiguration : io.openenterprise.springframework.boot.autoconfigure.spark.SparkAutoConfiguration() {

    @Autowired
    protected lateinit var streamingQueryListener: StreamingQueryListener

    @Bean
    @ConditionalOnBean(SparkConf::class)
    @Primary
    override fun sparkSession(sparkConf: SparkConf): SparkSession =
        super.sparkSession(sparkConf).apply {
            this.streams().addListener(streamingQueryListener)
        }
}