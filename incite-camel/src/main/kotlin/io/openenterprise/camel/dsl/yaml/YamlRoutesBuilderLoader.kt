package io.openenterprise.camel.dsl.yaml

import io.openenterprise.incite.data.domain.YamlRoute
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.model.RouteDefinition
import org.apache.commons.lang3.reflect.TypeUtils
import org.snakeyaml.engine.v2.api.LoadSettings
import org.snakeyaml.engine.v2.api.lowlevel.Compose
import javax.inject.Named

@Named
class YamlRoutesBuilderLoader: org.apache.camel.dsl.yaml.YamlRoutesBuilderLoader() {

    fun builder(yamlRoute: YamlRoute): RouteBuilder {
        val compose = Compose(LoadSettings.builder().build())
        val node = compose.composeString(yamlRoute.yaml).orElseThrow { IllegalArgumentException() }
        val resolvedItem = super.getDeserializationContext().mandatoryResolve(node).construct(node)

        assert (TypeUtils.isAssignable(resolvedItem::class.java, RouteDefinition::class.java))

        val routeBuilder = super.builder(node)
        routeBuilder.routeCollection.route().routeId(yamlRoute.id.toString())

        return routeBuilder
    }
}