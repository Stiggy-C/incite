package io.openenterprise.ignite.cache.query

import io.openenterprise.springframework.context.ApplicationContextUtils

interface Function {

    abstract class BaseCompanionObject {

        protected fun <T> getBean(clazz: Class<T>): T =
            ApplicationContextUtils.getApplicationContext()!!.getBean(clazz)
    }
}