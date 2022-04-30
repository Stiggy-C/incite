package io.openenterprise.incite.service

import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import java.time.Duration
import javax.inject.Inject
import javax.inject.Named

@Named
open class MessagingServiceImpl : MessagingService {

    @Inject
    private lateinit var ignite: Ignite

    @Inject
    private lateinit var igniteCluster: IgniteCluster

    override fun produce(topic: String, message: String, ordered: Boolean, replicated: Boolean) {
        val igniteMessaging =
            if (replicated) {
                ignite.message(igniteCluster.forPredicate { node -> !node.isClient && !node.isDaemon })
            } else {
                ignite.message(
                    igniteCluster.forNodeId(
                        igniteCluster.nodes().stream().filter { node -> !node.isClient && !node.isDaemon }
                            .findFirst()
                            .get()
                            .id()
                    )
                )
            }

        if (ordered) {
            igniteMessaging.sendOrdered(topic, message, Duration.ofSeconds(5).toMillis())
        } else {
            igniteMessaging.send(topic, message)
        }
    }
}