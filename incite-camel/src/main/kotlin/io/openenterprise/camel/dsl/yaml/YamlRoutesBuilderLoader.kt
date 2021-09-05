package io.openenterprise.camel.dsl.yaml

import org.apache.camel.builder.RouteBuilder
import org.apache.camel.dsl.yaml.YamlRoutesBuilderLoader
import org.apache.camel.model.RouteDefinition
import org.apache.commons.lang3.reflect.TypeUtils
import org.snakeyaml.engine.v2.api.LoadSettings
import org.snakeyaml.engine.v2.api.lowlevel.Compose
import javax.annotation.PostConstruct
import javax.inject.Named

@Named
class YamlRoutesBuilderLoader: YamlRoutesBuilderLoader() {

    fun builder(routeId: String, yaml: String): RouteBuilder {
        val compose = Compose(LoadSettings.builder().build())
        val node = compose.composeString(yaml).orElseThrow { IllegalArgumentException() }
        val resolvedItem = super.getDeserializationContext().mandatoryResolve(node).construct(node)

        assert (TypeUtils.isAssignable(resolvedItem::class.java, RouteDefinition::class.java))

        val routeBuilder = super.builder(node)
        routeBuilder.routeCollection.route().routeId(routeId)

        return routeBuilder
    }

    @PostConstruct
    fun postConstruct() {
        super.doBuild()
    }
}