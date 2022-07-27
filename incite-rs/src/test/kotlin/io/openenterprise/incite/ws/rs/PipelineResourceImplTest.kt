package io.openenterprise.incite.ws.rs

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import io.openenterprise.glassfish.jersey.spring.SpringBridgeFeature
import io.openenterprise.incite.data.domain.Pipeline
import io.openenterprise.incite.data.domain.JdbcSink
import io.openenterprise.incite.data.domain.JdbcSource
import io.openenterprise.incite.data.domain.RdbmsDatabase
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.ws.rs.ext.JsonMergePatchMessageBodyReader
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.glassfish.jersey.server.ResourceConfig
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.web.server.LocalServerPort
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.*
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.web.servlet.config.annotation.EnableWebMvc
import java.util.*
import javax.annotation.PostConstruct
import javax.ws.rs.ApplicationPath
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(
    classes = [PipelineResourceImplTest.Application::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
class PipelineResourceImplTest {

    @MockBean
    private lateinit var aggregateService: PipelineService

    @LocalServerPort
    private var localPort: Int = -1

    @Autowired
    private lateinit var testRestTemplate: TestRestTemplate

    @Test
    fun aggregate() {
    }

    @Test
    fun status() {
    }

    @Test
    fun create() {
    }

    @Test
    fun retrieve() {
    }

    @Test
    fun delete() {
    }

    @Test
    fun update() {
        val rdbmsDatabase = RdbmsDatabase()
        rdbmsDatabase.url = " jdbc:postgresql://localhost:5432/test_db"
        rdbmsDatabase.driverClass = "org.postgresql.Driver"
        rdbmsDatabase.username = "test_user"
        rdbmsDatabase.password = "test_pw"

        val jdbcSource = JdbcSource()
        jdbcSource.query = "select * from guest"
        jdbcSource.rdbmsDatabase = rdbmsDatabase

        val jdbcSink = JdbcSink()
        jdbcSink.id = UUID.randomUUID().toString()
        jdbcSink.rdbmsDatabase = rdbmsDatabase
        jdbcSink.table = "guest_aggregate_result"

        val pipeline = Pipeline()
        pipeline.id = UUID.randomUUID().toString()
        pipeline.sinks = Collections.singletonList(jdbcSink)
        pipeline.sources = Collections.singletonList(jdbcSource)

        Mockito.`when`(aggregateService.retrieve(pipeline.id!!)).thenReturn(pipeline)

        val httpHeaders = HttpHeaders()
        httpHeaders.set(HttpHeaders.CONTENT_TYPE, "application/merge-patch+json")

        val responseEntity = testRestTemplate.exchange(
            "http://localhost:$localPort/rs/aggregates/${pipeline.id}",
            HttpMethod.PATCH,
            HttpEntity("{\"description\":\"test_patch\"}", httpHeaders),
            Void::class.java
        )

        assertEquals(HttpStatus.NO_CONTENT, responseEntity.statusCode)
    }

    @ApplicationPath("/rs")
    @Order(Ordered.LOWEST_PRECEDENCE)
    @TestConfiguration
    class JerseyConfiguration : ResourceConfig() {

        @Autowired
        private lateinit var applicationContext: ApplicationContext

        @PostConstruct
        protected fun postConstruct() {
            // Providers:
            registerClasses(JsonMergePatchMessageBodyReader::class.java)
            registerInstances(SpringBridgeFeature(applicationContext))

            // Resource:
            registerClasses(PipelineResourceImpl::class.java)
        }
    }

    @EnableWebMvc
    @EnableWebSecurity
    @TestConfiguration
    class WebSecurityConfiguration: WebSecurityConfigurerAdapter() {

        override fun configure(http: HttpSecurity?) {
            http!!.authorizeRequests().antMatchers("/rs/*").permitAll()

            http.cors().and().csrf().disable().sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
        }
    }

    @SpringBootApplication
    @ComponentScan(
        excludeFilters = [ComponentScan.Filter(
            type = FilterType.REGEX,
            pattern = ["io.openenterprise.*"]
        )]
    )
    @EnableAutoConfiguration(exclude = [
        org.apache.ignite.springframework.boot.autoconfigure.IgniteAutoConfiguration::class
    ])
    @Import(JerseyConfiguration::class, WebSecurityConfiguration::class)
    class Application {

        @Bean
        protected fun coroutineScope(): CoroutineScope = CoroutineScope(Dispatchers.Default)

        @Bean
        protected fun objectMapper(): ObjectMapper = ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .findAndRegisterModules()
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
    }
}