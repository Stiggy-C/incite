package io.openenterprise.incite.data.domain

import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import javax.persistence.Converter

class KafkaCluster {

    lateinit var servers: String

}

@Converter
class KafkaClusterJsonAttributeConverter: AbstractJsonAttributeConverter<KafkaCluster>()