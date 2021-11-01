package io.openenterprise.glassfish.jersey.spring

import org.glassfish.hk2.api.ServiceLocator
import org.jvnet.hk2.spring.bridge.api.SpringBridge
import org.jvnet.hk2.spring.bridge.api.SpringIntoHK2Bridge
import org.springframework.context.ApplicationContext
import javax.inject.Inject
import javax.ws.rs.core.Feature
import javax.ws.rs.core.FeatureContext
import javax.ws.rs.ext.Provider

@Provider
class SpringBridgeFeature(private val applicationContext: ApplicationContext): Feature {

    @Inject
    private lateinit var serviceLocator: ServiceLocator

    override fun configure(featureContext: FeatureContext): Boolean {
        SpringBridge.getSpringBridge().initializeSpringBridge(serviceLocator)
        serviceLocator.getService(SpringIntoHK2Bridge::class.java).bridgeSpringBeanFactory(applicationContext)

        return true
    }
}