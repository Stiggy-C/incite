package io.openenterprise.incite.service

import io.openenterprise.incite.data.domain.Pipeline
import io.openenterprise.incite.PipelineContext
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.cache.query.annotations.QuerySqlFunction

interface PipelineService: AbstractMutableEntityService<Pipeline, String> {

    companion object {

        @JvmStatic
        @QuerySqlFunction(alias = "start")
        fun start(): String = TODO()
    }

    /**
     * Get the context by ID of the defined [Pipeline] (definition) instance.
     *
     * @param id The ID of the defined [Pipeline] instance.
     */
    fun getContext(id: String): PipelineContext?

    /**
     * Start the given [Pipeline].
     *
     * @param pipeline The [Pipeline]
     * @return The updated but not yet merged [Pipeline]
     */
    fun start(pipeline: Pipeline): Pipeline

    /**
     * Stop an [Pipeline] which is streaming. An [Pipeline] is considered as a streaming aggregate if it has one or
     * more [io.openenterprise.incite.data.domain.StreamingSink].
     *
     * @param id The id of the streaming aggregate
     * @throws UnsupportedOperationException If the corresponding aggregate is not a streaming aggregate
     */
    fun stopStreaming(id: String): Boolean
}