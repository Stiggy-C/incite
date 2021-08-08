package io.openenterprise.springframework.context

import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import javax.inject.Named

@Named
class ApplicationContextUtil : ApplicationContextAware {

    companion object {

        private var applicationContext: ApplicationContext? = null

        @JvmStatic
        fun getApplicationContext(): ApplicationContext? {
            return applicationContext
        }
    }

    @Synchronized
    override fun setApplicationContext(applicationContext: ApplicationContext) {
        if (ApplicationContextUtil.applicationContext == null) {
            ApplicationContextUtil.applicationContext = applicationContext
        }
    }
}