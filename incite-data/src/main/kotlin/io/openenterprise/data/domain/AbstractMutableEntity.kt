package io.openenterprise.data.domain

import java.io.Serializable
import java.time.OffsetDateTime
import javax.persistence.PostLoad
import javax.persistence.PrePersist
import javax.persistence.PreUpdate
import javax.persistence.Transient
import javax.validation.constraints.Max
import javax.validation.constraints.Size

abstract class AbstractMutableEntity<ID: Serializable> : AbstractEntity<ID>() {

    @Transient
    var before: AbstractMutableEntity<ID>? = null

    @Size(max = 320)
    open var updatedBy: String? = null

    open var updatedOffsetDateTime: OffsetDateTime? = null

    @PostLoad
    open fun postLoad() {
        before = this
    }

    @PreUpdate
    open fun preUpdate() {
        updatedOffsetDateTime = OffsetDateTime.now()
    }
}