package io.openenterprise.springframework.jdbc.support

import org.apache.commons.lang3.reflect.TypeUtils
import org.apache.ignite.IgniteJdbcThinDataSource
import org.springframework.jdbc.support.DatabaseStartupValidator
import javax.sql.DataSource

class IgniteStartupValidator: DatabaseStartupValidator() {

    init {
        super.setValidationQuery("SET LOCK_MODE = 3")
    }

    override fun setDataSource(dataSource: DataSource) {
        assert(TypeUtils.isAssignable(dataSource.javaClass, IgniteJdbcThinDataSource::class.java))

        super.setDataSource(dataSource)
    }
}