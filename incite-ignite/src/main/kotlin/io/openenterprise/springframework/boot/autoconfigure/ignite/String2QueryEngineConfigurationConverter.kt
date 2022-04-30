package io.openenterprise.springframework.boot.autoconfigure.ignite

import org.apache.ignite.configuration.QueryEngineConfiguration
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding
import org.springframework.core.convert.converter.Converter
import org.springframework.stereotype.Component

@Component
@ConfigurationPropertiesBinding
class String2QueryEngineConfigurationConverter : Converter<String, QueryEngineConfiguration> {

    override fun convert(source: String): QueryEngineConfiguration =
        Class.forName(source).newInstance() as QueryEngineConfiguration
}