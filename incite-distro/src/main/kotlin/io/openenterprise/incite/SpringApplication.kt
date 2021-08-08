package io.openenterprise.incite

import org.apache.ignite.springframework.boot.autoconfigure.IgniteAutoConfiguration
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer
import org.springframework.context.annotation.ComponentScan

@SpringBootConfiguration
@ComponentScan(basePackages = ["io.openenterprise.incite.context"])
@ImportAutoConfiguration(classes = [IgniteAutoConfiguration::class, ServletWebServerFactoryAutoConfiguration::class])
class SpringApplication : SpringBootServletInitializer() {

    companion object {

        @JvmStatic
        @kotlin.jvm.Throws(Exception::class)
        fun main(vararg args: String) {
            org.springframework.boot.SpringApplication.run(SpringApplication::class.java, *args)
        }
    }

    override fun configure(builder: SpringApplicationBuilder?): SpringApplicationBuilder {
        return builder!!.sources(SpringApplication::class.java)
    }
}