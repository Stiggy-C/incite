package io.openenterprise.testcontainers.containers


import com.github.dockerjava.api.command.InspectContainerResponse
import org.testcontainers.utility.DockerImageName
import java.lang.String
import kotlin.Boolean
import kotlin.arrayOf

class KafkaContainer: org.testcontainers.containers.KafkaContainer {

    constructor(): this(DockerImageName.parse("confluentinc/cp-kafka:7.2.2"))

    constructor(dockerImageName: DockerImageName) : super(dockerImageName)

    override fun containerIsStarted(containerInfo: InspectContainerResponse?, reused: Boolean) {
        val brokerAdvertisedListener = this.brokerAdvertisedListener(containerInfo)

        val bootstrapServer = if (this.isHostAccessible)
            "PLAINTEXT://host.testcontainers.internal:$KAFKA_PORT"
        else
            bootstrapServers

        val result = this.execInContainer(
            "kafka-configs",
            "--alter",
            "--bootstrap-server",
            brokerAdvertisedListener,
            "--entity-type",
            "brokers",
            "--entity-name",
            this.envMap["KAFKA_BROKER_ID"],
            "--add-config",
            "advertised.listeners=[" + String.join(",", bootstrapServer, brokerAdvertisedListener) + "]"
        )

        if (result.exitCode != 0) {
            throw IllegalStateException(result.toString())
        }
    }
}