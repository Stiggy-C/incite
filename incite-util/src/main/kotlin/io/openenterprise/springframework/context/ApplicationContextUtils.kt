package io.openenterprise.springframework.context

import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import javax.inject.Named

@Named
class ApplicationContextUtils : ApplicationContextAware {

    companion object {

        private var applicationContext: ApplicationContext? = null

        @JvmStatic
        fun getApplicationContext(): ApplicationContext? {
            return applicationContext
        }
    }

    @Synchronized
    override fun setApplicationContext(applicationContext: ApplicationContext) {
        if (ApplicationContextUtils.applicationContext == null) {
            ApplicationContextUtils.applicationContext = applicationContext
        }
    }
}