package io.openenterrpise.rs

import io.openenterprise.data.domain.AbstractMutableEntity
import java.io.Serializable
import javax.ws.rs.container.AsyncResponse

interface AbstractMutableEntityResource<T: AbstractMutableEntity<ID>, ID:Serializable> : AbstractEntityResource<T, ID> {

    fun update(id: ID, entity: T, asyncResponse: AsyncResponse)
}