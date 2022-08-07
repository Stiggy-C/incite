package io.openenterprise.incite.spark.sql.streaming

import io.openenterprise.incite.PipelineContext
import io.openenterprise.incite.PipelineContextUtils
import io.openenterprise.incite.PipelineContextUtils.Companion.getPipelineContextsLookup
import org.apache.ignite.Ignite
import org.slf4j.LoggerFactory
import java.util.*
import javax.inject.Inject
import javax.inject.Named

@Named
class StreamingQueryListener(
    @Inject private val ignite: Ignite
): org.apache.spark.sql.streaming.StreamingQueryListener() {

    companion object {

        @JvmStatic
        private val LOG = LoggerFactory.getLogger(StreamingQueryListener::class.java)
    }

    override fun onQueryStarted(event: QueryStartedEvent) {
        LOG.info("StreamingQuery, ${event.id()}, has been started")
    }

    override fun onQueryProgress(event: QueryProgressEvent) {
        LOG.info("StreamingQuery, ${event.progress().id()}, is processing")

        getPipelineContextsLookup(ignite)[event.progress().id()]?.status = PipelineContext.Status.PROCESSING
    }

    override fun onQueryTerminated(event: QueryTerminatedEvent) {
        val streamingQueryId = event.id()
        val status = if (event.exception().isEmpty)
            PipelineContext.Status.STOPPED
        else
            PipelineContext.Status.FAILED

        getPipelineContextsLookup(ignite)[streamingQueryId]?.status = status

        LOG.info("StreamingQuery, ${event.id()}, terminated with status, $status")
    }
}