package io.openenterprise.incite.context

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.io.File
import java.util.*
import javax.cache.Cache
import javax.cache.CacheManager
import javax.cache.configuration.MutableConfiguration

@Configuration
class JCacheConfiguration {

    @Autowired
    lateinit var cacheManager: CacheManager

    /*@Bean("mlModelsCache")
    fun mlModelsCache(): Cache<UUID, File> {
        val mutableConfiguration: MutableConfiguration<UUID, File> =
            MutableConfiguration<UUID, File>()
        mutableConfiguration.setTypes(UUID::class.java, File::class.java)

        return cacheManager.createCache("mlModels", mutableConfiguration)
    }*/
}