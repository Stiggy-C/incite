package io.openenterprise.camel.dsl.yaml

import org.apache.camel.builder.RouteBuilder
import org.apache.camel.dsl.yaml.common.YamlDeserializationContext
import org.apache.camel.model.RouteDefinition
import org.apache.commons.lang3.reflect.TypeUtils
import org.snakeyaml.engine.v2.api.LoadSettings
import org.snakeyaml.engine.v2.api.lowlevel.Compose
import javax.annotation.PostConstruct
import javax.inject.Named

@Named
class YamlRoutesBuilderLoader: org.apache.camel.dsl.yaml.YamlRoutesBuilderLoader() {

    fun builder(routeId: String, yaml: String): RouteBuilder {
        val loadSettings = LoadSettings.builder().build()

        val compose = Compose(loadSettings)
        val node = compose.composeString(yaml).orElseThrow { IllegalArgumentException() }
        val yamlDeserializationContext = YamlDeserializationContext(loadSettings)

        val routeBuilder = super.builder(yamlDeserializationContext, node)
        routeBuilder.routeCollection.route().routeId(routeId)

        return routeBuilder
    }

    @PostConstruct
    fun postConstruct() {
        super.doBuild()
    }
}