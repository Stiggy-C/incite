package io.openenterprise.camel.dsl.yaml

import org.apache.camel.builder.RouteBuilder
import org.snakeyaml.engine.v2.api.LoadSettings
import org.snakeyaml.engine.v2.api.lowlevel.Compose

class YamlRoutesBuilderLoader: org.apache.camel.dsl.yaml.YamlRoutesBuilderLoader() {

    fun builder(yamlString: String): RouteBuilder {
        val compose = Compose(LoadSettings.builder().build())
        val node = compose.composeString(yamlString).orElseThrow { IllegalArgumentException() }

        return super.builder(node)
    }
}