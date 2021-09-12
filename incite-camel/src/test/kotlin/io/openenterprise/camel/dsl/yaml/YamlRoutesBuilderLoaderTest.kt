package io.openenterprise.camel.dsl.yaml

import org.apache.camel.CamelContext
import org.apache.camel.spring.SpringCamelContext
import org.apache.ignite.Ignite
import org.apache.ignite.internal.IgniteKernal
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner
import java.util.*
import javax.inject.Inject

@RunWith(SpringRunner::class)
@ContextConfiguration(classes = [YamlRoutesBuilderLoaderTest.TestConfiguration::class])
class YamlRoutesBuilderLoaderTest {

    @Inject
    lateinit var yamlRoutesBuilderLoader: YamlRoutesBuilderLoader

    @Test
    fun testBasic() {
        val routeId = UUID.randomUUID().toString();
        val yaml = "route:\n" +
                "    from: \"timer:yaml?period=3s\"\n" +
                "    steps:\n" +
                "      - set-body:\n" +
                "          simple: \"Timer fired \${header.CamelTimerCounter} times.\"\n" +
                "      - to:\n" +
                "          uri: \"log:yaml\"\n" +
                "          parameters:\n" +
                "            show-body-type: false\n" +
                "            show-exchange-pattern: false"

        val routeBuilder = yamlRoutesBuilderLoader.builder(routeId, yaml)

        assertNotNull(routeBuilder)
    }

    @Test
    fun testComplex() {
        val routeId = UUID.randomUUID().toString();
        val yaml = "route:\n" +
                "    from: \"ignite-messaging:sample_event_topic?ignite='#{ignite}'\"\n" +
                "    steps:\n" +
                "        - idempotent-consumer:\n" +
                "            expression: \n" +
                "                header: \"CamelIgniteMessagingUUID\"\n" +
                "            message-id-repository-ref: \"jdbcOrphanLockAwareIdempotentRepository\"\n" +
                "        - unmarshal:\n" +
                "            json:\n" +
                "                library: Jackson\n" +
                "        - choice:\n" +
                "            when: \n" +
                "                - expression:\n" +
                "                    spel: \"#{request.body.type == 'guest_complain'}\"\n" +
                "                    steps:\n" +
                "                        - set-body:\n" +
                "                            simple: \"insert into sample_event(guestId, content, isComplain, createdDateTime) values ('\${body.guestId}', '\${body.content}', true, '\${body.createdDateTime}')\"\n" +
                "            otherwise:\n" +
                "                steps:\n" +
                "                    - set-body:\n" +
                "                        simple: \"insert into sample_event(guestId, content, isComplain, createdDateTime) values ('\${body.guestId}', '\${body.content}', false, '\${body.createdDateTime}')\"\n" +
                "        - to: \"jdbc:igniteJdbcThinDataSource\""

        val routeBuilder = yamlRoutesBuilderLoader.builder(routeId, yaml)

        assertNotNull(routeBuilder)
    }

    @Configuration
    class TestConfiguration {

        @Bean
        fun ignite(): Ignite {
            return IgniteKernal()
        }

        @Bean
        fun springCamelContext(applicationContext: ApplicationContext): SpringCamelContext {
            return SpringCamelContext(applicationContext)
        }

        @Bean
        fun yamlRoutesBuilderLoader(camelContext: CamelContext): YamlRoutesBuilderLoader {
            val yamlRoutesBuilderLoader = YamlRoutesBuilderLoader()
            yamlRoutesBuilderLoader.camelContext = camelContext

            return yamlRoutesBuilderLoader
        }
    }

}