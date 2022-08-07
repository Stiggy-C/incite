package io.openenterprise.incite

import com.google.common.collect.Maps
import org.apache.ignite.Ignite
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class PipelineContextUtils {

    companion object {

        @JvmStatic
        @SuppressWarnings("UNCHECKED_CAST")
        fun getPipelineContexts(ignite: Ignite): MutableMap<String, PipelineContext> {
            val nodeLocalMap = ignite.cluster().nodeLocalMap<String, Any>()

            nodeLocalMap.putIfAbsent("pipelineContexts", Maps.newConcurrentMap<String, PipelineContext>())

            return nodeLocalMap["pipelineContexts"] as ConcurrentHashMap<String, PipelineContext>
        }

        @JvmStatic
        @SuppressWarnings("UNCHECKED_CAST")
        fun getPipelineContextsLookup(ignite: Ignite): MutableMap<UUID, PipelineContext> {
            val nodeLocalMap = ignite.cluster().nodeLocalMap<String, Any>()
            nodeLocalMap.putIfAbsent("pipelineContextLookup", Maps.newConcurrentMap<UUID, PipelineContext>())

            return nodeLocalMap["pipelineContextLookup"] as ConcurrentHashMap<UUID, PipelineContext>
        }
    }

}