package io.openenterprise.incite

import org.apache.commons.lang3.StringUtils
import org.testcontainers.containers.KafkaContainer

sealed class TestUtils {

    companion object {

        @JvmStatic
        fun manipulateKafkaBootstrapServers(kafkaContainer: KafkaContainer): String =
            StringUtils.replace(
                kafkaContainer.bootstrapServers, "localhost",
                "host.testcontainers.internal"
            )

    }
}